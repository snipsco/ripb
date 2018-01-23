use std::any::{Any, TypeId};
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::mpsc::{Sender, Receiver, channel};
use std::thread;

#[derive(Clone)]
pub struct Bus {
    tx: Sender<BusTask>,
}

enum BusTask {
    Publish {
        type_id: TypeId,
        message: Box<Any + Send>,
        cloner: Cloner,
        worker: Worker,
    },
    RegisterSubscriber { subscriber: Sender<SubscriberTask> },
}

pub struct Subscriber {
    tx: Sender<SubscriberTask>
}

enum SubscriberTask {
    Receive {
        type_id: TypeId,
        message: Box<Any + Send>,
        worker: Arc<Worker>,
    },
    RegisterCallback {
        type_id: TypeId,
        callback: Box<Any + Send>,
    },
}

struct Cloner {
    callback: Box<Fn(&Box<Any + Send>) -> (Box<Any + Send>) + Send>
}

impl Cloner {
    pub fn of<T: Send + Sync + 'static>() -> Self {
        Self {
            // TODO error handler remove this unwrap
            callback: Box::new(|payload| Box::new(Arc::clone(payload.downcast_ref::<Arc<T>>().unwrap())) as Box<Any + Send>)
        }
    }

    pub fn call(&self, to_clone: &Box<Any + Send>) -> Box<Any + Send> {
        (self.callback)(to_clone)
    }
}

struct Worker {
    // fn (callback, payload)
    worker: Box<Fn(Arc<Box<Any>>, Box<Any>) + Send + Sync>
}

impl Worker {
    pub fn of<T: Send + Sync + 'static>() -> Self {
        Self {
            worker: Box::new(|c: Arc<Box<Any>>, payload: Box<Any>| {
                let callback = c.downcast_ref::<Callback<T>>().unwrap();
                let message = payload.downcast_ref::<Arc<T>>().unwrap();
                callback.call(message);
            })
        }
    }


    pub fn call(&self, callback: Arc<Box<Any>>, payload: Box<Any>) {
        (self.worker)(callback, payload)
    }
}

struct Callback<M: Send + Sync + 'static> {
    callback: Box<Fn(&M) -> () + Send>
}

impl<M: Send + Sync + 'static> Callback<M> {
    pub fn new<F: 'static>(handler: F) -> Callback<M> where F: Fn(&M) -> () + Send {
        Callback { callback: Box::new(handler) }
    }

    pub fn call(&self, arg: &M) { (self.callback)(arg) }
}


impl Bus {
    pub fn new() -> Self {
        let (tx, rx) = channel();

        thread::spawn(move || {
            let mut subscribers: Vec<Sender<SubscriberTask>> = vec![];
            loop {
                match rx.recv() {
                    Ok(BusTask::Publish { type_id, message, cloner, worker }) => {
                        let callback = Arc::new(worker);
                        for subscriber in subscribers.iter() {
                            subscriber.send(SubscriberTask::Receive {
                                type_id,
                                message: cloner.call(&message),
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

    pub fn create_subscriber(&self) -> Subscriber {
        let subscriber = Subscriber::new();
        self.tx.send(BusTask::RegisterSubscriber { subscriber: subscriber.tx.clone() }).unwrap(); // TODO error handling
        subscriber
    }

    pub fn publish<M: Send + Sync + 'static>(&self, message: M) {
        self.tx.send(BusTask::Publish {
            type_id: TypeId::of::<M>(),
            message: Box::new(Arc::new(message)),
            cloner: Cloner::of::<M>(),
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
            let mut callbacks: HashMap<TypeId, Arc<Box<Any>>> = HashMap::new();
            loop {
                match rx.recv() {
                    Ok(SubscriberTask::RegisterCallback { type_id, callback }) => {
                        callbacks.insert(type_id, Arc::new(callback));
                    }
                    Ok(SubscriberTask::Receive { type_id, message, worker }) => {
                        callbacks.get(&type_id).map(|it| worker.call(Arc::clone(it), message));
                    }
                    /* sender is dead */
                    Err(_) => break
                }
            }
        });
    }

    pub fn on_message<F, M>(&mut self, callback: F) where F: Fn(&M) + Send + 'static, M: Send + Sync + 'static {

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
        let mut subscriber = bus.create_subscriber();

        let (tx, rx) = channel();

        subscriber.on_message(move |_: &()| tx.send(()).unwrap());

        bus.publish(());

        assert!(rx.recv_timeout(Duration::from_secs(1)).is_ok());
    }


    #[test]
    fn can_send_a_simple_message_to_2_subscriber2() {
        let bus = Bus::new();
        let mut subscriber = bus.create_subscriber();
        let mut subscriber2 = bus.create_subscriber();

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
        let mut subscriber = bus.create_subscriber();

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
        let mut subscriber = bus.create_subscriber();

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
        let mut subscriber = bus.create_subscriber();

        let (tx, rx) = channel();

        subscriber.on_message(move |_: &()| tx.send(()).unwrap());

        bus.publish(());
        let bus2 = bus.clone();
        bus2.publish(());

        assert!(rx.recv_timeout(Duration::from_secs(1)).is_ok());
        assert!(rx.recv_timeout(Duration::from_secs(1)).is_ok());
    }
}


