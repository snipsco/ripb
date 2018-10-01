//! This crates provides an implementation of a lock-free, type-safe, in-process bus.
//!
//! # Guarantees
//!
//! - In order delivery : messages published on the bus are received in the same order by a given
//! subscriber. This is not guaranteed across multiple subscribers. IE if you send Messages m1 and
//! m2 on a bus with subscribers s1 and s2, both s1 and s2 will receive the messages in the order
//! m1, m2, but s1 may receive m2 before s2 has received m1.
//!
//!
//! # Implementation
//!
//! Current implementation uses [`crossbeam-channel`]s, a fixed number of threads [`Any`] and
//! [`TypeId`] are used to to be able to expose a type-safe api

extern crate crossbeam_channel;
extern crate num_cpus;

use crossbeam_channel::{Receiver, Sender};
use std::any::{Any, TypeId};
use std::cell::Cell;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::thread;

//mod legacy;

/// An in process bus.
///
/// You can create a bus with the [`new`] method, then create new [`Subscriber`]s with the
/// [`create_subscriber`] method, and push some messages on the bus with the [`publish`] method.
/// If you need to send messages from multiple threads, [`clone`] the bus and use one instance per
/// thread
#[derive(Clone)]
pub struct Bus {
    control: Sender<BusTask>,
    subscriber_id_source: Arc<AtomicUsize>,
}

/// A subscriber to a [`Bus`].
///
/// Subscribers are created with the method [`Bus.create_subscriber`]
///
/// Register new callbacks with the [`on_message`] method, callback will live until the subscriber
/// is dropped. If you need more control on callback lifecycle use [`on_message_with_token`] that
/// will give you a [`SubscriptionToken`] you can use to [`unsubscribe`] a callback.
pub struct Subscriber {
    subscriber_id: usize,
    control: Sender<BusTask>,
    callback_id_source: AtomicUsize,
}

enum BusTask {
    Publish {
        type_id: TypeId,
        message: Arc<BoxedMessage>,
        worker: Worker,
    },
    RegisterSubscriber { subscriber: SubscriberState, subscriber_id: usize },
    UnregisterSubscriber { subscriber_id: usize },
    RegisterSubscriberCallback {
        subscriber_id: usize,
        callback_id: usize,
        type_id: TypeId,
        callback: BoxedCallback,
    },
    UnregisterSubscriberCallback {
        subscriber_id: usize,
        callback_id: usize,
        type_id: TypeId,
    },
}

/// A message on must be [`Send`] and [`Sync`] to be sent on the bus.
pub trait Message: Send + Sync {}

impl<T: Send + Sync> Message for T {}

type BoxedMessage = Box<Any + Send + Sync>;
type BoxedCallback = Box<Any + Send>;

enum SubscriberTask {
    Receive {
        type_id: TypeId,
        message: Arc<BoxedMessage>,
        worker: Arc<Worker>,
    },
    RegisterCallback {
        callback_id: usize,
        type_id: TypeId,
        callback: BoxedCallback,
    },
    UnregisterCallback {
        callback_id: usize,
        type_id: TypeId,
    },
}

struct Worker {
    worker: Box<Fn(&BoxedCallback, Arc<BoxedMessage>) + Send + Sync>
}

impl Worker {
    pub fn of<T: Message + 'static>() -> Self {
        Self {
            worker: Box::new(|c: &BoxedCallback, payload: Arc<BoxedMessage>| {
                let callback = c.downcast_ref::<Callback<T>>()
                    .expect("Could not downcast_ref for callback, this is a bug in ripb");
                let message = Any::downcast_ref::<T>(&**payload)
                    .expect("Could not downcast_ref for message, this is a bug in ripb");
                callback.call(message);
            })
        }
    }

    pub fn call(&self, callback: &BoxedCallback, payload: Arc<BoxedMessage>) {
        (self.worker)(callback, payload)
    }
}

struct Callback<M: Message> {
    callback: Box<Fn(&M) -> () + Send>
}

impl<M: Message> Callback<M> {
    pub fn new<F: 'static>(handler: F) -> Callback<M> where F: Fn(&M) -> () + Send {
        Callback { callback: Box::new(handler) }
    }

    pub fn call(&self, arg: &M) { (self.callback)(arg) }
}

struct BusState {
    subs: HashMap<usize, Cell<Receiver<SubscriberState>>>,
    tasks: Receiver<BusTask>,
    thread_count: usize,
}

struct SubscriberState {
    callbacks: HashMap<TypeId, Vec<(usize, BoxedCallback)>>,
}

enum BusWorkerTask {
    ManageBusState {
        state: BusState,
    },
    ManageSubscriberState {
        state: Receiver<SubscriberState>,
        task: SubscriberTask,
        next_state: Sender<SubscriberState>,
    },
    Stop,
}

struct BusWorker {
    tasks: Receiver<BusWorkerTask>,
    backlog: Sender<BusWorkerTask>,
}

impl BusWorker {
    fn run(&self) {
        loop {
            match self.tasks.recv() {
                Some(BusWorkerTask::ManageBusState { state }) => self.manage_bus_state(state),
                Some(BusWorkerTask::ManageSubscriberState { state, task, next_state }) =>
                    self.manage_subscriber_state(state, task, next_state),
                Some(BusWorkerTask::Stop) => break,
                None => panic!("None in bus run, this is a bug in ripb")
            }
        }
    }

    fn manage_bus_state(&self, state: BusState) {
        let BusState { mut subs, tasks, thread_count } = state;
        match tasks.recv() {
            Some(BusTask::Publish { type_id, message, worker }) => {
                let worker = Arc::new(worker);

                for sub in subs.values_mut() {
                    let (next_state, new_sub) = crossbeam_channel::bounded(1);
                    let task = SubscriberTask::Receive {
                        type_id,
                        message: Arc::clone(&message),
                        worker: Arc::clone(&worker),
                    };
                    let state = sub.replace(new_sub);
                    self.backlog.send(BusWorkerTask::ManageSubscriberState {
                        state,
                        task,
                        next_state,
                    })
                }
            }
            Some(BusTask::RegisterSubscriber { subscriber, subscriber_id }) => {
                let (next_state, new_sub) = crossbeam_channel::bounded(1);
                subs.insert(subscriber_id, Cell::new(new_sub));
                next_state.send(subscriber)
            }
            Some(BusTask::UnregisterSubscriber { subscriber_id }) => {
                subs.remove(&subscriber_id);
            }
            Some(BusTask::RegisterSubscriberCallback {
                     subscriber_id, callback_id, type_id, callback
                 }) => {
                let (next_state, new_sub) = crossbeam_channel::bounded(1);
                if let Some(state) = subs.insert(subscriber_id, Cell::new(new_sub)) {
                    let task = SubscriberTask::RegisterCallback { callback_id, type_id, callback };
                    self.backlog.send(BusWorkerTask::ManageSubscriberState {
                        state: state.into_inner(),
                        task,
                        next_state,
                    })
                }
            }
            Some(BusTask::UnregisterSubscriberCallback { subscriber_id, callback_id, type_id }) => {
                let (next_state, new_sub) = crossbeam_channel::bounded(1);
                if let Some(state) = subs.insert(subscriber_id, Cell::new(new_sub)) {
                    let task = SubscriberTask::UnregisterCallback { callback_id, type_id };
                    self.backlog.send(BusWorkerTask::ManageSubscriberState {
                        state: state.into_inner(),
                        task,
                        next_state,
                    })
                }
            }
            None => {
                if self.backlog.len() == 0 {
                    // No new message can arrive on the bus, and the backlog is empty, so lets stop
                    for _ in 0..thread_count {
                        self.backlog.send(BusWorkerTask::Stop)
                    }
                }
            }
        }
        self.backlog.send(BusWorkerTask::ManageBusState {
            state: BusState { subs, tasks, thread_count }
        })
    }

    fn manage_subscriber_state(&self,
                               state: Receiver<SubscriberState>,
                               task: SubscriberTask,
                               next_state: Sender<SubscriberState>) {
        match state.try_recv() {
            Some(mut state) => {
                match task {
                    SubscriberTask::Receive { type_id, message, worker } => {
                        state.callbacks.get(&type_id)
                            .map(|its| for (_, it) in its {
                                worker.call(it, Arc::clone(&message))
                            });
                    }
                    SubscriberTask::RegisterCallback { type_id, callback_id, callback } => {
                        state.callbacks.entry(type_id)
                            .or_insert_with(|| vec![])
                            .push((callback_id, callback));
                    }
                    SubscriberTask::UnregisterCallback { type_id, callback_id } => {
                        state.callbacks.get_mut(&type_id)
                            .expect("Trying to unregister a callback on a type not seen yet")
                            .retain(|(it, _)| *it != callback_id);
                    }
                }
                next_state.send(state)
            }
            // TODO be smart and avoid a busy loop here if there are only messages for a subscriber
            // TODO that is taking a bit of time
            None => self.backlog.send(BusWorkerTask::ManageSubscriberState {
                state,
                task,
                next_state,
            })
        }
    }
}

impl Bus {
    /// Create a new bus, with a thread count equal to the number of CPUs
    pub fn new() -> Self {
        Self::with_thread_count(num_cpus::get())
    }

    /// Create a new bus with the given thread count
    pub fn with_thread_count(thread_count: usize) -> Self {
        let (backlog, tasks) = crossbeam_channel::unbounded();
        let (control, bus_tasks) = crossbeam_channel::unbounded();

        for i in 0..thread_count {
            let worker = BusWorker { tasks: tasks.clone(), backlog: backlog.clone() };
            let _ = thread::Builder::new()
                .name(format!("ripb.worker{}", i))
                .spawn(move || worker.run());
        }
        backlog.send(BusWorkerTask::ManageBusState {
            state: BusState { subs: HashMap::new(), tasks: bus_tasks, thread_count }
        });

        Bus { control, subscriber_id_source: Arc::new(AtomicUsize::from(0)) }
    }

    /// Create a new subscriber for this bus
    pub fn create_subscriber(&self) -> Subscriber {
        let subscriber_id = self.subscriber_id_source.fetch_add(1, Ordering::Relaxed);
        let control = self.control.clone();
        let callback_id_source = AtomicUsize::from(0);
        self.control.send(BusTask::RegisterSubscriber {
            subscriber: SubscriberState { callbacks: HashMap::new() },
            subscriber_id,
        });
        Subscriber { subscriber_id, control, callback_id_source }
    }

    /// Publish a new message on this bus
    pub fn publish<M: Message + 'static>(&self, message: M) {
        self.control.send(BusTask::Publish {
            type_id: TypeId::of::<M>(),
            message: Arc::new(Box::new(message)),
            worker: Worker::of::<M>(),
        })
    }
}

impl Subscriber {
    fn on_message_inner<F, M>(&self, callback: F) -> usize
        where F: Fn(&M) + Send + 'static, M: Message + 'static {
        let callback_id = self.callback_id_source.fetch_add(1, Ordering::Relaxed);
        self.control.send(BusTask::RegisterSubscriberCallback {
            subscriber_id: self.subscriber_id,
            callback_id,
            callback: Box::new(Callback::new(callback)),
            type_id: TypeId::of::<M>(),
        });
        callback_id
    }

    /// Register a new callback to be called each time a message of the given type is published on
    /// the bus, callback lives as until the `Subscriber` is dropped
    pub fn on_message<F, M>(&self, callback: F)
        where F: Fn(&M) + Send + 'static, M: Message + 'static {
        self.on_message_inner(callback);
    }

    /// Register a new callback to be called each time a message of the given type is published on
    /// the bus, callback lives as until the `SubscriptionToken` is dropped, `unsubscribe` is
    /// called on it or the `Subscriber` is dropped
    pub fn on_message_with_token<F, M>(&self, callback: F) -> SubscriptionToken
        where F: Fn(&M) + Send + 'static, M: Message + 'static {
        SubscriptionToken {
            subscriber_id: self.subscriber_id,
            callback_id: self.on_message_inner(callback),
            type_id: TypeId::of::<M>(),
            control: self.control.clone(),
        }
    }
}

impl Drop for Subscriber {
    fn drop(&mut self) {
        self.control.send(BusTask::UnregisterSubscriber { subscriber_id: self.subscriber_id })
    }
}

#[must_use]
pub struct SubscriptionToken {
    type_id: TypeId,
    subscriber_id: usize,
    callback_id: usize,
    control: Sender<BusTask>,
}

impl SubscriptionToken {
    pub fn unsubscribe(&self) {
        self.control.send(BusTask::UnregisterSubscriberCallback {
            type_id: self.type_id,
            callback_id: self.callback_id,
            subscriber_id: self.subscriber_id,
        })
    }
}

impl Drop for SubscriptionToken {
    fn drop(&mut self) {
        self.unsubscribe()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::mpsc::channel;
    use std::thread;
    use std::time::Duration;
    use super::*;

    #[test]
    fn can_send_a_simple_message_to_a_subscriber() {
        let bus = Bus::new();
        let subscriber = bus.create_subscriber();

        let (tx, rx) = channel();

        subscriber.on_message(move |_: &()| tx.send(()).unwrap());

        bus.publish(());

        assert!(rx.recv_timeout(Duration::from_secs(1)).is_ok());
    }


    #[test]
    fn can_send_a_simple_message_to_2_subscribers() {
        let bus = Bus::new();
        let subscriber = bus.create_subscriber();
        let subscriber2 = bus.create_subscriber();

        let (tx, rx) = channel();
        let (tx2, rx2) = channel();

        subscriber.on_message(move |_: &()| tx.send(()).unwrap());
        subscriber2.on_message(move |_: &()| tx2.send(()).unwrap());

        bus.publish(());

        assert!(rx.recv_timeout(Duration::from_secs(1)).is_ok());
        assert!(rx2.recv_timeout(Duration::from_secs(1)).is_ok());
    }


    #[test]
    fn can_send_a_complex_message_to_a_subscriber() {
        let bus = Bus::new();
        let subscriber = bus.create_subscriber();

        let (tx, rx) = channel();

        struct Message { payload: String }

        subscriber.on_message(move |m: &Message| tx.send(m.payload.clone()).unwrap());

        bus.publish(Message { payload: "hello world".into() });

        let result = rx.recv_timeout(Duration::from_secs(1));

        assert!(result.is_ok());
        assert_eq!(result.unwrap(), "hello world".to_string());
    }

    #[test]
    fn can_send_simple_messages_to_a_subscriber_from_multiple_threads() {
        let bus = Bus::new();
        let subscriber = bus.create_subscriber();

        let (tx, rx) = channel();

        subscriber.on_message(move |_: &()| tx.send(()).unwrap());

        bus.publish(());
        thread::spawn(move || bus.publish(()));

        assert!(rx.recv_timeout(Duration::from_secs(1)).is_ok());
        assert!(rx.recv_timeout(Duration::from_secs(1)).is_ok());
    }

    #[test]
    fn can_send_simple_messages_to_a_subscriber_from_cloned_instance() {
        let bus = Bus::new();
        let subscriber = bus.create_subscriber();

        let (tx, rx) = channel();

        subscriber.on_message(move |_: &()| tx.send(()).unwrap());

        bus.publish(());
        let bus2 = bus.clone();
        bus2.publish(());

        assert!(rx.recv_timeout(Duration::from_secs(1)).is_ok());
        assert!(rx.recv_timeout(Duration::from_secs(1)).is_ok());
    }

    #[test]
    fn can_unsubscribe_callbacks() {
        let bus = Bus::new();
        let subscriber = bus.create_subscriber();

        let (tx, rx) = channel();

        let token = subscriber.on_message_with_token(move |_: &()| tx.send(()).unwrap());
        bus.publish(());
        assert!(rx.recv_timeout(Duration::from_secs(1)).is_ok());

        token.unsubscribe();
        bus.publish(());
        assert!(rx.recv_timeout(Duration::from_secs(1)).is_err());
    }

    #[test]
    fn cannot_receive_messages_in_a_dropped_subscriber() {
        let bus = Bus::new();
        let (tx, rx) = channel();
        {
            let subscriber = bus.create_subscriber();

            subscriber.on_message(move |_: &()| tx.send(()).unwrap());
            bus.publish(());
            assert!(rx.recv_timeout(Duration::from_secs(1)).is_ok());
            // subscriber is dropped here
        };

        bus.publish(());

        assert!(rx.recv_timeout(Duration::from_secs(1)).is_err());
    }

    #[test]
    fn can_unsubscribe_a_token_for_a_dropped_subscriber_without_crashing() {
        fn drop_subscriber(_sub: Subscriber) {}
        let bus = Bus::new();
        let (tx, rx) = channel();
        let subscriber = bus.create_subscriber();
        let token = subscriber.on_message_with_token(move |_: &()| tx.send(()).unwrap());
        bus.publish(());
        assert!(rx.recv_timeout(Duration::from_secs(1)).is_ok());

        drop_subscriber(subscriber);
        bus.publish(());

        assert!(rx.recv_timeout(Duration::from_secs(1)).is_err());

        token.unsubscribe(); // we don't want that to panic
    }

    #[test]
    fn dropping_subscribers_drops_the_corresponding_subscription() {
        fn drop_subscriber(_sub: Subscriber) {}

        let bus = Bus::new();
        let (tx, rx) = channel();
        let (tx2, rx2) = channel();
        let (tx3, rx3) = channel();

        let subscriber = bus.create_subscriber();
        subscriber.on_message(move |_: &()| tx.send(()).unwrap());
        let subscriber2 = bus.create_subscriber();
        subscriber2.on_message(move |_: &()| tx2.send(()).unwrap());
        let subscriber3 = bus.create_subscriber();
        subscriber3.on_message(move |_: &()| tx3.send(()).unwrap());

        bus.publish(());
        assert!(rx.recv_timeout(Duration::from_secs(1)).is_ok());
        assert!(rx2.recv_timeout(Duration::from_secs(1)).is_ok());
        assert!(rx3.recv_timeout(Duration::from_secs(1)).is_ok());
        drop_subscriber(subscriber);
        drop_subscriber(subscriber3);

        bus.publish(());

        assert!(rx.recv_timeout(Duration::from_secs(1)).is_err());
        assert!(rx2.recv_timeout(Duration::from_secs(1)).is_ok());
        assert!(rx3.recv_timeout(Duration::from_secs(1)).is_err());
    }

    #[test]
    fn can_register_multiple_message_handlers_for_the_same_message_on_a_single_receiver() {
        fn drop_token(_sub: SubscriptionToken) {}

        let bus = Bus::new();
        let (tx, rx) = channel();
        let (tx2, rx2) = channel();
        let (tx3, rx3) = channel();

        let subscriber = bus.create_subscriber();
        let t = subscriber.on_message_with_token(move |_: &()| tx.send(()).unwrap());
        subscriber.on_message(move |_: &()| tx2.send(()).unwrap());
        let t3 = subscriber.on_message_with_token(move |_: &()| tx3.send(()).unwrap());

        bus.publish(());
        assert!(rx.recv_timeout(Duration::from_secs(1)).is_ok());
        assert!(rx2.recv_timeout(Duration::from_secs(1)).is_ok());
        assert!(rx3.recv_timeout(Duration::from_secs(1)).is_ok());

        t.unsubscribe();
        drop_token(t3);

        bus.publish(());

        assert!(rx.recv_timeout(Duration::from_secs(1)).is_err());
        assert!(rx2.recv_timeout(Duration::from_secs(1)).is_ok());
        assert!(rx3.recv_timeout(Duration::from_secs(1)).is_err());
    }
}


