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
use crossbeam_channel::{Receiver, RecvError, RecvTimeoutError, Select, Sender, TryRecvError};
use std::any::{Any, TypeId};
use std::cell::Cell;
use std::collections::HashMap;
use std::fmt::Debug;
use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread;

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

#[derive(Debug)]
enum BusTask {
    Publish {
        type_id: TypeId,
        message: Arc<BoxedMessage>,
        worker: Worker,
    },
    RegisterSubscriber {
        subscriber: SubscriberState,
        subscriber_id: usize,
    },
    UnregisterSubscriber {
        subscriber_id: usize,
    },
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
    Stop {
        halted_tx: Sender<()>,
    },
}

/// A message on must be [`Send`] and [`Sync`] to be sent on the bus.
pub trait Message: Send + Sync {}

impl<T: Send + Sync> Message for T {}

type BoxedMessage = Box<Any + Send + Sync>;
type BoxedCallback = Box<Any + Send>;

#[derive(Debug)]
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
    worker: Box<Fn(&BoxedCallback, Arc<BoxedMessage>) + Send + Sync>,
}

impl Debug for Worker {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> Result<(), std::fmt::Error> {
        write!(f, "<Worker>")
    }
}

impl Worker {
    pub fn of<T: Message + 'static>() -> Self {
        Self {
            worker: Box::new(|c: &BoxedCallback, payload: Arc<BoxedMessage>| {
                let callback = c
                    .downcast_ref::<Callback<T>>()
                    .expect("Could not downcast_ref for callback, this is a bug in ripb");
                let message = Any::downcast_ref::<T>(&**payload)
                    .expect("Could not downcast_ref for message, this is a bug in ripb");
                callback.call(message);
            }),
        }
    }

    pub fn call(&self, callback: &BoxedCallback, payload: Arc<BoxedMessage>) {
        (self.worker)(callback, payload)
    }
}

struct Callback<M: Message> {
    callback: Box<Fn(&M) -> () + Send>,
}

impl<M: Message> Callback<M> {
    pub fn new<F: 'static>(handler: F) -> Callback<M>
    where
        F: Fn(&M) -> () + Send,
    {
        Callback {
            callback: Box::new(handler),
        }
    }

    pub fn call(&self, arg: &M) {
        (self.callback)(arg)
    }
}

struct BusState {
    subs: HashMap<usize, Cell<Receiver<SubscriberState>>>,
    tasks: Receiver<BusTask>,
    thread_count: usize,
}

impl Debug for BusState {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> Result<(), std::fmt::Error> {
        write!(
            f,
            "BusState {{ subs: <{}>, tasks: {:?}, thread_count: {} }}",
            self.subs.len(),
            self.tasks,
            self.thread_count
        )
    }
}

struct SubscriberState {
    callbacks: HashMap<TypeId, Vec<(usize, BoxedCallback)>>,
}

impl Debug for SubscriberState {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> Result<(), std::fmt::Error> {
        write!(
            f,
            "SubscriberState {{ callbacks: {:?} }}",
            self.callbacks
                .iter()
                .map(|it| (it.0.clone(), format!("<{} callbacks>", it.1.len())))
                .collect::<HashMap<TypeId, String>>()
        )
    }
}

#[derive(Debug)]
enum BusWorkerTask {
    ManageBusState {
        state: BusState,
    },
    ManageSubscriberState {
        subscriber_id: usize,
        state: Receiver<SubscriberState>,
        task: SubscriberTask,
        next_state: Sender<SubscriberState>,
    },
    ManageSlowSubscribersStates {
        subscriber_ids: Vec<usize>,
        states: Vec<Receiver<SubscriberState>>,
        tasks: Vec<SubscriberTask>,
        next_states: Vec<Sender<SubscriberState>>,
    },
    Stop {
        state: BusState,
        halted_tx: Option<Sender<()>>,
    },
}

struct BusWorker {
    id: usize,
    tasks: Receiver<BusWorkerTask>,
    backlog: Sender<BusWorkerTask>,
}

impl BusWorker {
    fn run(&self) {
        log::info!("worker {} started", self.id);
        loop {
            match self.tasks.recv() {
                Ok(task) => {
                    if !self.handle_task(task) {
                        break;
                    }
                }
                Err(RecvError {}) => panic!("Task/backlog channel closed, this should not happen"),
            }
        }
        log::info!("worker {} fisnished", self.id);
    }

    fn handle_task(&self, task: BusWorkerTask) -> bool {
        log::debug!("bus worker {} handling {:?}", self.id, task);
        return match task {
            BusWorkerTask::ManageBusState { state } => self.manage_bus_state(state),
            BusWorkerTask::ManageSubscriberState {
                subscriber_id,
                state,
                task,
                next_state,
            } => self.manage_subscriber_state(subscriber_id, state, task, next_state),
            BusWorkerTask::ManageSlowSubscribersStates {
                subscriber_ids,
                states,
                tasks,
                next_states,
            } => self.manage_slow_subscribers_states(subscriber_ids, states, tasks, next_states),
            BusWorkerTask::Stop { state, halted_tx } => self.handle_stop(state, halted_tx),
        };
    }

    fn handle_stop(&self, mut state: BusState, halted_tx: Option<Sender<()>>) -> bool {
        log::debug!("bus worker {} handling a stop task", self.id);

        let should_continue = self.tasks.len() > 0;

        if !should_continue {
            state.thread_count -= 1;
            log::debug!("threadcount is now {}", state.thread_count);
        }

        log::debug!(
            "bus worker {} remaining subs : {}",
            self.id,
            state.subs.len(),
        );

        if state.thread_count > 0 {
            // we own a receiver so sending should not fail
            self.backlog
                .send(BusWorkerTask::Stop { state, halted_tx })
                .unwrap();
        } else {
            log::debug!("bus is done");
            drop(state);
            if let Some(halted_tx) = halted_tx {
                halted_tx.send(()).unwrap();
            }
        }

        should_continue
    }

    fn manage_bus_state(&self, state: BusState) -> bool {
        log::debug!("bus worker {} managing the bus state", self.id);

        let BusState {
            mut subs,
            tasks,
            thread_count,
        } = state;
        let mut should_continue = true;
        let mut halted_tx_opt = None;

        let task = tasks
            .recv()
            .expect("bus task channel was closed without sending a stop command");

        log::debug!(
            "bus worker {} managing the bus state with task {:?}",
            self.id,
            task,
        );

        match task {
            BusTask::Publish {
                type_id,
                message,
                worker,
            } => {
                let worker = Arc::new(worker);

                for (subscriber_id, sub) in subs.iter_mut() {
                    let (next_state, new_sub) = crossbeam_channel::bounded(1);
                    let task = SubscriberTask::Receive {
                        type_id,
                        message: Arc::clone(&message),
                        worker: Arc::clone(&worker),
                    };
                    let state = sub.replace(new_sub);
                    // we own a receiver so sending should not fail
                    self.backlog
                        .send(BusWorkerTask::ManageSubscriberState {
                            subscriber_id: *subscriber_id,
                            state,
                            task,
                            next_state,
                        })
                        .unwrap();
                }
            }
            BusTask::RegisterSubscriber {
                subscriber,
                subscriber_id,
            } => {
                let (next_state, new_sub) = crossbeam_channel::bounded(1);
                // receiver is still alive as it is in the scope
                next_state.send(subscriber).unwrap();
                subs.insert(subscriber_id, Cell::new(new_sub));
            }
            BusTask::UnregisterSubscriber { subscriber_id } => {
                // make sure all tasks for this subscriber have finished executing
                // the recv here is blocking, if it poses some performance problems it can be
                // sub in a new bus task
                subs.remove(&subscriber_id)
                    .expect("trying to remove a non existing subscriber")
                    .get_mut()
                    .recv()
                    .expect("subscriber channel should not be close");
            }
            BusTask::RegisterSubscriberCallback {
                subscriber_id,
                callback_id,
                type_id,
                callback,
            } => {
                let (next_state, new_sub) = crossbeam_channel::bounded(1);
                if let Some(state) = subs.insert(subscriber_id, Cell::new(new_sub)) {
                    let task = SubscriberTask::RegisterCallback {
                        callback_id,
                        type_id,
                        callback,
                    };
                    // we own a receiver so sending should not fail
                    self.backlog
                        .send(BusWorkerTask::ManageSubscriberState {
                            subscriber_id,
                            state: state.into_inner(),
                            task,
                            next_state,
                        })
                        .unwrap();
                } else {
                    panic!("trying to register a callback for an unknown subscriber")
                }
            }
            BusTask::UnregisterSubscriberCallback {
                subscriber_id,
                callback_id,
                type_id,
            } => {
                let (next_state, new_sub) = crossbeam_channel::bounded(1);
                if let Some(state) = subs.insert(subscriber_id, Cell::new(new_sub)) {
                    let task = SubscriberTask::UnregisterCallback {
                        callback_id,
                        type_id,
                    };
                    // we own a receiver so sending should not fail
                    self.backlog
                        .send(BusWorkerTask::ManageSubscriberState {
                            subscriber_id,
                            state: state.into_inner(),
                            task,
                            next_state,
                        })
                        .unwrap();
                } else {
                    panic!("trying to unregister a callback for an unknown subscriber")
                }
            }
            BusTask::Stop { halted_tx } => {
                should_continue = false;
                halted_tx_opt = Some(halted_tx)
            }
        }

        let state = BusState {
            subs,
            tasks,
            thread_count,
        };

        log::debug!(
            "bus worker {} remaining subs : {}",
            self.id,
            state.subs.len(),
        );

        if should_continue {
            // we own a receiver so sending should not fail
            self.backlog
                .send(BusWorkerTask::ManageBusState { state })
                .unwrap();
        } else {
            // we own a receiver so sending should not fail
            self.backlog
                .send(BusWorkerTask::Stop {
                    state,
                    halted_tx: halted_tx_opt,
                })
                .unwrap()
        }
        return true;
    }

    fn manage_subscriber_state(
        &self,
        subscriber_id: usize,
        state: Receiver<SubscriberState>,
        task: SubscriberTask,
        next_state: Sender<SubscriberState>,
    ) -> bool {
        log::debug!(
            "bus worker {} trying to manage state of subscriber {}",
            self.id,
            subscriber_id,
        );
        match state.try_recv() {
            Ok(state) => self.perform_subscriber_task(subscriber_id, state, task, next_state),
            // If we simply repost the task there are some cases where we'll en up with a busy loop
            // (e.g. if one subscriber is taking its time and there are only tasks for it in the
            // backlog). We don't want that so we need to handle slow subscribers a little more
            // carefully
            Err(TryRecvError::Empty) => self
                .backlog
                .send(BusWorkerTask::ManageSlowSubscribersStates {
                    subscriber_ids: vec![subscriber_id],
                    states: vec![state],
                    tasks: vec![task],
                    next_states: vec![next_state],
                })
                .expect("backlog channel was disconnected"),
            Err(TryRecvError::Disconnected) => {
                panic!("Channel for subscriber state is disconnected")
            }
        }
        return true;
    }

    fn perform_subscriber_task(
        &self,
        subscriber_id: usize,
        mut state: SubscriberState,
        task: SubscriberTask,
        next_state: Sender<SubscriberState>,
    ) {
        log::debug!(
            "bus worker {} performing task {:?} for subscriber {} with state {:?}",
            self.id,
            task,
            subscriber_id,
            state,
        );
        match task {
            SubscriberTask::Receive {
                type_id,
                message,
                worker,
            } => {
                state.callbacks.get(&type_id).map(|its| {
                    for (_, it) in its {
                        worker.call(it, Arc::clone(&message))
                    }
                });
            }
            SubscriberTask::RegisterCallback {
                type_id,
                callback_id,
                callback,
            } => {
                state
                    .callbacks
                    .entry(type_id)
                    .or_insert_with(|| vec![])
                    .push((callback_id, callback));
            }
            SubscriberTask::UnregisterCallback {
                type_id,
                callback_id,
            } => {
                state
                    .callbacks
                    .get_mut(&type_id)
                    .expect("Trying to unregister a callback on a type not seen yet")
                    .retain(|(it, _)| *it != callback_id);
            }
        }

        next_state
            .send(state)
            .expect("state channel for subscriber should not be disconnected");
    }

    fn manage_slow_subscribers_states(
        &self,
        mut subscriber_ids: Vec<usize>,
        mut states: Vec<Receiver<SubscriberState>>,
        mut tasks: Vec<SubscriberTask>,
        mut next_states: Vec<Sender<SubscriberState>>,
    ) -> bool {
        log::debug!(
            "bus worker {} trying to manage states of {} slow subscribers",
            self.id,
            subscriber_ids.len(),
        );
        enum Action {
            Exec {
                state: SubscriberState,
                index: usize,
            },
            Merge {
                other_ids: Vec<usize>,
                other_states: Vec<Receiver<SubscriberState>>,
                other_tasks: Vec<SubscriberTask>,
                other_next_states: Vec<Sender<SubscriberState>>,
            },
            Other {
                task: BusWorkerTask,
            },
        }

        let action = {
            let mut select = Select::new();
            for state in &states {
                select.recv(state);
            }

            select.recv(&self.tasks);

            let oper = select.select();
            let index = oper.index();

            if index == states.len() {
                let task = oper.recv(&self.tasks);
                match task {
                    Ok(BusWorkerTask::ManageSlowSubscribersStates {
                        subscriber_ids: other_ids,
                        states: other_states,
                        tasks: other_tasks,
                        next_states: other_next_states,
                    }) => Action::Merge {
                        other_ids,
                        other_states,
                        other_tasks,
                        other_next_states,
                    },
                    Ok(task) => Action::Other { task },
                    Err(RecvError {}) => {
                        panic!("Task/backlog channel closed, this should not happen")
                    }
                }
            } else {
                let state = oper.recv(&states[index]);
                match state {
                    Ok(state) => Action::Exec { state, index },
                    Err(RecvError {}) => {
                        panic!("state channel for slow subscriber should not be disconnected")
                    }
                }
            }
        };

        return match action {
            Action::Exec { state, index } => {
                states.remove(index);
                let subscriber_id = subscriber_ids.remove(index);
                log::debug!(
                    "bus worker {} managing state of subscriber {}",
                    self.id,
                    subscriber_id,
                );
                let task = tasks.remove(index);
                let next_state = next_states.remove(index);
                if states.len() > 0 {
                    // we own a receiver so sending should not fail
                    self.backlog
                        .send(BusWorkerTask::ManageSlowSubscribersStates {
                            subscriber_ids,
                            states,
                            tasks,
                            next_states,
                        })
                        .expect("backlog channel was disconnected");
                }
                self.perform_subscriber_task(subscriber_id, state, task, next_state);
                true
            }
            Action::Merge {
                mut other_ids,
                mut other_states,
                mut other_tasks,
                mut other_next_states,
            } => {
                log::debug!(
                    "bus worker {} adding {} subscribers to task already containing {}",
                    self.id,
                    other_ids.len(),
                    subscriber_ids.len(),
                );
                subscriber_ids.append(&mut other_ids);
                states.append(&mut other_states);
                tasks.append(&mut other_tasks);
                next_states.append(&mut other_next_states);
                // we own a receiver so sending should not fail
                self.backlog
                    .send(BusWorkerTask::ManageSlowSubscribersStates {
                        subscriber_ids,
                        states,
                        tasks,
                        next_states,
                    })
                    .expect("backlog channel was disconnected");
                true
            }
            Action::Other { task } => {
                log::debug!("bus worker {} executing another task", self.id);
                // we own a receiver so sending should not fail
                self.backlog
                    .send(BusWorkerTask::ManageSlowSubscribersStates {
                        subscriber_ids,
                        states,
                        tasks,
                        next_states,
                    })
                    .expect("backlog channel was disconnected");
                self.handle_task(task)
            }
        };
    }
}

/// Error produced when a bus operation is impossible. Getting such an error the bus is dead
#[derive(Debug)]
pub struct DeadBusError;

impl Bus {
    /// Create a new bus, with a thread count equal to the number of CPUs
    pub fn new() -> Self {
        Self::with_thread_count(
            NonZeroUsize::new(num_cpus::get()).expect("Number of CPU should be non zero"),
        )
    }

    /// Create a new bus with the given thread count
    pub fn with_thread_count(thread_count: NonZeroUsize) -> Self {
        let (backlog, tasks) = crossbeam_channel::unbounded();
        let (control, bus_tasks) = crossbeam_channel::unbounded();
        let thread_count = thread_count.get();

        for id in 0..thread_count {
            let worker = BusWorker {
                id,
                tasks: tasks.clone(),
                backlog: backlog.clone(),
            };
            let _ = thread::Builder::new()
                .name(format!("ripb.worker{}", id))
                .spawn(move || worker.run());
        }

        // receiver still in scope so sending should not fail
        backlog
            .send(BusWorkerTask::ManageBusState {
                state: BusState {
                    subs: HashMap::new(),
                    tasks: bus_tasks,
                    thread_count,
                },
            })
            .expect("backlog channel was disconnected");

        Bus {
            control,
            subscriber_id_source: Arc::new(AtomicUsize::from(0)),
        }
    }

    /// Create a new subscriber for this bus
    pub fn create_subscriber(&self) -> Subscriber {
        let subscriber_id = self.subscriber_id_source.fetch_add(1, Ordering::Relaxed);
        let control = self.control.clone();
        let callback_id_source = AtomicUsize::from(0);
        self.control
            .send(BusTask::RegisterSubscriber {
                subscriber: SubscriberState {
                    callbacks: HashMap::new(),
                },
                subscriber_id,
            })
            .expect("could not communicate with the bus, did a worker thread panic ?");
        Subscriber {
            subscriber_id,
            control,
            callback_id_source,
        }
    }

    /// Publish a new message on this bus
    pub fn publish<M: Message + 'static>(&self, message: M) {
        self.control
            .send(BusTask::Publish {
                type_id: TypeId::of::<M>(),
                message: Arc::new(Box::new(message)),
                worker: Worker::of::<M>(),
            })
            .expect("could not communicate with the bus, did a worker thread panic ?")
    }
}

impl Drop for Bus {
    fn drop(&mut self) {
        let arc_count = Arc::strong_count(&self.subscriber_id_source);
        log::debug!("Dropping a Bus, arc count is {}", arc_count);
        if arc_count == 1 {
            log::debug!("This is the last instance for this bus, killing it");
            let (halted_tx, halted_rx) = crossbeam_channel::bounded(1);
            self.control
                .send(BusTask::Stop { halted_tx })
                .expect("control channel was disconnected");
            match halted_rx.recv_timeout(std::time::Duration::from_secs(5)) {
                Err(RecvTimeoutError::Timeout) => {
                    panic!("bus didn't stop properly after 5 seconds")
                }
                Err(RecvTimeoutError::Disconnected) => panic!("stop channel was closed"),
                Ok(()) => {}
            }
        }
    }
}

impl Subscriber {
    fn on_message_inner<F, M>(&self, callback: F) -> Result<usize, DeadBusError>
    where
        F: Fn(&M) + Send + 'static,
        M: Message + 'static,
    {
        let callback_id = self.callback_id_source.fetch_add(1, Ordering::Relaxed);
        self.control
            .send(BusTask::RegisterSubscriberCallback {
                subscriber_id: self.subscriber_id,
                callback_id,
                callback: Box::new(Callback::new(callback)),
                type_id: TypeId::of::<M>(),
            })
            .map_err(|_| DeadBusError)?;
        Ok(callback_id)
    }

    /// Register a new callback to be called each time a message of the given type is published on
    /// the bus, callback lives as until the `Subscriber` is dropped
    pub fn on_message<F, M>(&self, callback: F) -> Result<(), DeadBusError>
    where
        F: Fn(&M) + Send + 'static,
        M: Message + 'static,
    {
        self.on_message_inner(callback)?;
        Ok(())
    }

    /// Register a new callback to be called each time a message of the given type is published on
    /// the bus, callback lives as until the `SubscriptionToken` is dropped, `unsubscribe` is
    /// called on it or the `Subscriber` is dropped
    pub fn on_message_with_token<F, M>(
        &self,
        callback: F,
    ) -> Result<SubscriptionToken, DeadBusError>
    where
        F: Fn(&M) + Send + 'static,
        M: Message + 'static,
    {
        Ok(SubscriptionToken {
            subscriber_id: self.subscriber_id,
            callback_id: self.on_message_inner(callback)?,
            type_id: TypeId::of::<M>(),
            subscriber: &self,
        })
    }
}

impl Drop for Subscriber {
    fn drop(&mut self) {
        // this may fail if the bus is already stopped, but in that case we don't care
        let _ = self.control.send(BusTask::UnregisterSubscriber {
            subscriber_id: self.subscriber_id,
        });
    }
}

#[must_use]
pub struct SubscriptionToken<'a> {
    type_id: TypeId,
    subscriber_id: usize,
    callback_id: usize,
    subscriber: &'a Subscriber,
}

impl<'a> SubscriptionToken<'a> {
    pub fn unsubscribe(self) {
        // unsubscription is done when drop occurs
        drop(self)
    }
}

impl<'a> Drop for SubscriptionToken<'a> {
    fn drop(&mut self) {
        // this may fail if the bus is already stopped, but in that case we don't care
        let _ = self
            .subscriber
            .control
            .send(BusTask::UnregisterSubscriberCallback {
                type_id: self.type_id,
                callback_id: self.callback_id,
                subscriber_id: self.subscriber_id,
            });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::mpsc::channel;
    use std::thread;
    use std::time::Duration;

    #[test]
    fn can_send_a_simple_message_to_a_subscriber() {
        let bus = Bus::new();
        let subscriber = bus.create_subscriber();

        let (tx, rx) = channel();

        subscriber
            .on_message(move |_: &()| tx.send(()).unwrap())
            .unwrap();

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

        subscriber
            .on_message(move |_: &()| tx.send(()).unwrap())
            .unwrap();
        subscriber2
            .on_message(move |_: &()| tx2.send(()).unwrap())
            .unwrap();

        bus.publish(());

        assert!(rx.recv_timeout(Duration::from_secs(1)).is_ok());
        assert!(rx2.recv_timeout(Duration::from_secs(1)).is_ok());
    }

    #[test]
    fn can_send_a_complex_message_to_a_subscriber() {
        let bus = Bus::new();
        let subscriber = bus.create_subscriber();

        let (tx, rx) = channel();

        struct Message {
            payload: String,
        }

        subscriber
            .on_message(move |m: &Message| tx.send(m.payload.clone()).unwrap())
            .unwrap();

        bus.publish(Message {
            payload: "hello world".into(),
        });

        let result = rx.recv_timeout(Duration::from_secs(1));

        assert!(result.is_ok());
        assert_eq!(result.unwrap(), "hello world".to_string());
    }

    #[test]
    fn can_send_simple_messages_to_a_subscriber_from_multiple_threads() {
        let bus = Bus::new();
        let subscriber = bus.create_subscriber();

        let (tx, rx) = channel();

        subscriber
            .on_message(move |_: &()| tx.send(()).unwrap())
            .unwrap();

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

        subscriber
            .on_message(move |_: &()| tx.send(()).unwrap())
            .unwrap();

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

        let token = subscriber
            .on_message_with_token(move |_: &()| tx.send(()).unwrap())
            .unwrap();
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

            subscriber
                .on_message(move |_: &()| tx.send(()).unwrap())
                .unwrap();
            bus.publish(());
            assert!(rx.recv_timeout(Duration::from_secs(1)).is_ok());
            // subscriber is dropped here
        };

        bus.publish(());

        assert!(rx.recv_timeout(Duration::from_secs(1)).is_err());
    }

    #[test]
    fn dropping_subscribers_drops_the_corresponding_subscription() {
        fn drop_subscriber(_sub: Subscriber) {}

        let bus = Bus::new();
        let (tx, rx) = channel();
        let (tx2, rx2) = channel();
        let (tx3, rx3) = channel();

        let subscriber = bus.create_subscriber();
        subscriber
            .on_message(move |_: &()| tx.send(()).unwrap())
            .unwrap();
        let subscriber2 = bus.create_subscriber();
        subscriber2
            .on_message(move |_: &()| tx2.send(()).unwrap())
            .unwrap();
        let subscriber3 = bus.create_subscriber();
        subscriber3
            .on_message(move |_: &()| tx3.send(()).unwrap())
            .unwrap();

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
        let t = subscriber
            .on_message_with_token(move |_: &()| tx.send(()).unwrap())
            .unwrap();
        subscriber
            .on_message(move |_: &()| tx2.send(()).unwrap())
            .unwrap();
        let t3 = subscriber
            .on_message_with_token(move |_: &()| tx3.send(()).unwrap())
            .unwrap();

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

    #[test]
    fn subscriber_on_a_dropped_bus_should_generate_dead_bus_error_on_subscribe() {
        let bus = Bus::new();
        let subscriber = bus.create_subscriber();
        drop(bus);
        let r = subscriber.on_message(|_: &()| {});
        assert!(r.is_err())
    }

    #[test]
    fn can_drop_the_bus_while_it_is_still_working() {
        let bus = Bus::new();
        let subscriber = bus.create_subscriber();
        let (tx, rx) = crossbeam_channel::unbounded();
        subscriber
            .on_message(move |_: &()| tx.send(()).unwrap())
            .unwrap();

        for _ in 0..100 {
            bus.publish(())
        }

        drop(bus);

        assert_eq!(rx.len(), 100)
    }

    #[test]
    #[ignore]
    fn no_busyloop() {
        // Watch cpu usage during the 2 first secs of executing this, it should be nearly 0%
        // you may want to up the sleep time to make this more visible

        let bus = Bus::with_thread_count(NonZeroUsize::new(4).unwrap());

        let subscriber = bus.create_subscriber();
        let (tx, rx) = channel();

        subscriber
            .on_message(move |_: &()| {
                ::std::thread::sleep(::std::time::Duration::from_secs(1));
                tx.send(()).unwrap();
            })
            .unwrap();

        bus.publish(());
        bus.publish(());
        bus.publish(());

        assert_eq!(rx.recv().expect("recv 1"), ());
        assert_eq!(rx.recv().expect("recv 2"), ());
        assert_eq!(rx.recv().expect("recv 3"), ());
    }
}
