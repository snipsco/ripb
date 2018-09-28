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
//! Current implementation uses [`channel`]s, a [`thread`] to run the bus, and a [`thread`] per
//! subscriber. [`Any`] and [`TypeId`] are used to to be able to expose a type-safe api

use std::any::{Any, TypeId};
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc::{channel, Receiver, Sender};
use std::thread;

/// An in process bus.
///
/// You can create a bus with the [`new`] method, then create new [`Subscriber`]s with the
/// [`create_subscriber`] method, and push some messages on the bus with the [`publish`] method.
/// If you need to send messages from multiple threads, [`clone`] the bus and use one instance per
/// thread
#[derive(Clone)]
pub struct Bus {
    tx: Sender<BusTask>,
}

/// A subscriber to a [`Bus`].
///
/// Subscribers are created with the method [`Bus.create_subscriber`]
///
/// Register new callbacks with the [`on_message`] method, callback will leav until the subscriber
/// is dropped. If you need more control on callback lifecycle use [`on_message_with_token`] that
/// will give you a [`SubscriptionToken`] you can use to [`unsubscribe`] a callback.
pub struct Subscriber {
    tx: Sender<SubscriberTask>,
    callback_id_source: AtomicUsize,
}

enum BusTask {
    Publish {
        type_id: TypeId,
        message: Arc<BoxedMessage>,
        worker: Worker,
    },
    RegisterSubscriber { subscriber: Sender<SubscriberTask> },
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
        id: usize,
        type_id: TypeId,
        callback: BoxedCallback,
    },
    UnregisterCallback {
        id: usize,
        type_id: TypeId,
    },
    Stop,
}

struct Worker {
    worker: Box<Fn(Arc<BoxedCallback>, Arc<BoxedMessage>) + Send + Sync>
}

impl Worker {
    pub fn of<T: Message + 'static>() -> Self {
        Self {
            worker: Box::new(|c: Arc<BoxedCallback>, payload: Arc<BoxedMessage>| {
                let callback = c.downcast_ref::<Callback<T>>()
                    .expect("Could not downcast_ref for callback, this is a bug in ripb");
                let message = Any::downcast_ref::<T>(&**payload)
                    .expect("Could not downcast_ref for message, this is a bug in ripb");
                callback.call(message);
            })
        }
    }

    pub fn call(&self, callback: Arc<BoxedCallback>, payload: Arc<BoxedMessage>) {
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

impl Bus {
    /// Create a new bus
    pub fn new() -> Self {
        let (tx, rx) = channel();

        thread::spawn(move || {
            let mut subscribers: Vec<Sender<SubscriberTask>> = vec![];
            let mut indexes_to_remove = vec![];
            loop {
                match rx.recv() {
                    Ok(BusTask::Publish { type_id, message, worker }) => {
                        let callback = Arc::new(worker);
                        for (index, subscriber) in subscribers.iter().enumerate() {
                            if subscriber.send(SubscriberTask::Receive {
                                type_id,
                                message: Arc::clone(&message),
                                worker: Arc::clone(&callback),
                            }).is_err() {
                                indexes_to_remove.push(index);
                            }
                        }
                        while let Some(index) = indexes_to_remove.pop() {
                            subscribers.remove(index);
                        }
                    }
                    Ok(BusTask::RegisterSubscriber { subscriber }) => subscribers.push(subscriber),
                    /* senders are dead, we won't receive anything new, let's stop */
                    Err(_) => break,
                }
            }
        }
        );

        Self { tx }
    }

    /// Create a new subscriber for this bus
    pub fn create_subscriber(&self) -> Subscriber {
        let subscriber = Subscriber::new();
        self.tx.send(BusTask::RegisterSubscriber { subscriber: subscriber.tx.clone() })
            .expect("Could not create subscriber, bus is dead"); // TODO error handling
        subscriber
    }

    /// Publish a new message on this bus
    pub fn publish<M: Message + 'static>(&self, message: M) {
        self.tx.send(BusTask::Publish {
            type_id: TypeId::of::<M>(),
            message: Arc::new(Box::new(message)),
            worker: Worker::of::<M>(),
        }).expect("Could not publish, bus is dead"); //TODO error handling remove this expect()
    }
}

impl Subscriber {
    fn new() -> Self {
        let (tx, rx) = channel();
        let result = Self {
            tx,
            callback_id_source: AtomicUsize::from(0),
        };
        result.start(rx);
        result
    }

    fn start(&self, rx: Receiver<SubscriberTask>) {
        thread::spawn(move || {
            let mut callbacks: HashMap<TypeId, Vec<(usize, Arc<BoxedCallback>)>> = HashMap::new();
            loop {
                match rx.recv() {
                    Ok(SubscriberTask::RegisterCallback { type_id, id, callback }) => {
                        callbacks.entry(type_id)
                            .or_insert_with(|| vec![])
                            .push((id, Arc::new(callback)));
                    }
                    Ok(SubscriberTask::UnregisterCallback { type_id, id }) => {
                        callbacks.get_mut(&type_id)
                            .expect("Trying to unregister a callback on a type not seen yet")
                            .retain(|(it, _)| *it != id);
                    }
                    Ok(SubscriberTask::Receive { type_id, message, worker }) => {
                        callbacks.get(&type_id)
                            .map(|its| for (_, it) in its {
                                worker.call(Arc::clone(it), Arc::clone(&message))
                            });
                    }
                    Ok(SubscriberTask::Stop) => break,
                    /* sender is dead, bus is stopped, let's stop too */
                    Err(_) => break
                }
            }
        });
    }

    fn on_message_inner<F, M>(&self, callback: F) -> usize where F: Fn(&M) + Send + 'static, M: Message + 'static {
        let id = self.callback_id_source.fetch_add(1, Ordering::Relaxed);
        self.tx.send(SubscriberTask::RegisterCallback {
            id,
            callback: Box::new(Callback::new(callback)),
            type_id: TypeId::of::<M>(),
        }).expect("Could not register callback, bus is dead"); // TODO error handling
        id
    }

    /// Register a new callback to be called each time a message of the given type is published on
    /// the bus, callback lives as until the `Subscriber` is dropped
    pub fn on_message<F, M>(&self, callback: F) where F: Fn(&M) + Send + 'static, M: Message + 'static {
        self.on_message_inner(callback);
    }

    /// Register a new callback to be called each time a message of the given type is published on
    /// the bus, callback lives as until the `SubscriptionToken` is dropped, `unsubscribe` is
    /// called on it or the `Subscriber` is dropped
    pub fn on_message_with_token<F, M>(&self, callback: F) -> SubscriptionToken where F: Fn(&M) + Send + 'static, M: Message + 'static {
        SubscriptionToken {
            id: self.on_message_inner(callback),
            type_id: TypeId::of::<M>(),
            tx: self.tx.clone(),
        }
    }
}

impl Drop for Subscriber {
    fn drop(&mut self) {
        let _ = self.tx.send(SubscriberTask::Stop);
    }
}

#[must_use]
pub struct SubscriptionToken {
    type_id: TypeId,
    id: usize,
    tx: Sender<SubscriberTask>,
}

impl SubscriptionToken {
    pub fn unsubscribe(&self) {
        // if sending does't work here, the worker thread is dead so we're already unsubscribed
        // and we can ignore the error
        let _ = self.tx.send(SubscriberTask::UnregisterCallback { type_id: self.type_id, id: self.id });
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
    fn can_mutliple_message_handlers_for_othe_same_message_on_a_single_receiver() {
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


