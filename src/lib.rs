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
use std::sync::mpsc::{Sender, Receiver, channel};
use std::thread;


/// An in process bus.
///
/// You can create a bus with the [`new`] method, then create new [`Subscriber`]s with the
/// [`create_subscriber`] method, and push some messages on the bus with the [`publish`] method.
/// If you need to send messages from multiple threads, [`clone`] the bus and use one instance per thread
#[derive(Clone)]
pub struct Bus {
    tx: Sender<BusTask>,
}

/// A subscriber to a [`Bus`].
///
/// Subscribers are created with the method [`Bus.create_subscriber`]
///
/// Register new callbacks with the [`on_message`] method
pub struct Subscriber {
    tx: Sender<SubscriberTask>
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
        type_id: TypeId,
        callback: BoxedCallback,
    },
}

struct Worker {
    worker: Box<Fn(Arc<BoxedCallback>, Arc<BoxedMessage>) + Send + Sync>
}

impl Worker {
    pub fn of<T: Message + 'static>() -> Self {
        Self {
            worker: Box::new(|c: Arc<BoxedCallback>, payload: Arc<BoxedMessage>| {
                let callback = c.downcast_ref::<Callback<T>>().unwrap();
                let message = Any::downcast_ref::<T>(&**payload).unwrap();
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
            loop {
                match rx.recv() {
                    Ok(BusTask::Publish { type_id, message, worker }) => {
                        let callback = Arc::new(worker);
                        for subscriber in subscribers.iter() {
                            subscriber.send(SubscriberTask::Receive {
                                type_id,
                                message: Arc::clone(&message),
                                worker: Arc::clone(&callback),
                            }).unwrap() // TODO what should we do if this fails ? remove the subscriber ?
                        }
                    }
                    Ok(BusTask::RegisterSubscriber { subscriber }) => subscribers.push(subscriber),
                    /* senders are dead */
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
        self.tx.send(BusTask::RegisterSubscriber { subscriber: subscriber.tx.clone() }).unwrap(); // TODO error handling
        subscriber
    }

    /// Publish a new message on this bus
    pub fn publish<M: Message + 'static>(&self, message: M) {
        self.tx.send(BusTask::Publish {
            type_id: TypeId::of::<M>(),
            message: Arc::new(Box::new(message)),
            worker: Worker::of::<M>(),
        }).unwrap(); //TODO error handling remove this unwrap()
    }
}

impl Subscriber {
    fn new() -> Self {
        let (tx, rx) = channel();
        let result = Self {
            tx,
        };
        result.start(rx);
        result
    }


    fn start(&self, rx: Receiver<SubscriberTask>) {
        thread::spawn(move || {
            let mut callbacks: HashMap<TypeId, Arc<BoxedCallback>> = HashMap::new();
            loop {
                match rx.recv() {
                    Ok(SubscriberTask::RegisterCallback { type_id, callback }) => {
                        callbacks.insert(type_id, Arc::new(callback));
                    }
                    Ok(SubscriberTask::Receive { type_id, message, worker }) => {
                        callbacks.get(&type_id).map(|it| worker.call(Arc::clone(it), Arc::clone(&message)));
                    }
                    /* sender is dead */
                    Err(_) => break
                }
            }
        });
    }

    /// Register a new callback to be called each time a message of the given type is published on
    /// the bus
    pub fn on_message<F, M>(&self, callback: F) where F: Fn(&M) + Send + 'static, M: Message + 'static {

        // TODO remove this unwrap
        self.tx.send(SubscriberTask::RegisterCallback {
            callback: Box::new(Callback::new(callback)),
            type_id: TypeId::of::<M>(),
        }).unwrap()
    }
}


#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::mpsc::channel;
    use std::time::Duration;
    use std::thread;

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
    fn can_send_a_simple_message_to_2_subscriber2() {
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
}


