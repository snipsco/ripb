extern crate boxfnonce;

use std::any::{Any, TypeId};
use std::collections::{HashMap, HashSet};
use std::fmt::{Debug, Formatter};
use std::hash::{Hash, Hasher};
use std::sync::{Arc, Mutex};
use std::sync::mpsc::{Sender, Receiver, channel};
use std::thread;

use boxfnonce::SendBoxFnOnce;

pub struct Bus {
    inner: Arc<Mutex<InnerBus>>,
    current_id: Mutex<u32>,
}

struct InnerBus {
    subscribers: HashSet<Arc<InnerSubscriber>>
}

pub struct Subscriber {
    inner: Arc<InnerSubscriber>,
}

enum SubscriberTask {
    Run {
        type_id: TypeId,
        payload: Box<Any + Send>,
        // fn (callback, payload)
        callback: SendBoxFnOnce<(Arc<Box<Any>>, Box<Any>)>,
    },
    RegisterCallback {
        type_id: TypeId,
        callback: Box<Any + Send>,
    },
}

struct InnerSubscriber {
    id: u32,
    tx: Sender<SubscriberTask>,
}

impl Eq for InnerSubscriber {}

impl PartialEq for InnerSubscriber {
    fn eq(&self, other: &InnerSubscriber) -> bool {
        self.id == other.id
    }
}

impl Hash for InnerSubscriber {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.id.hash(state)
    }
}

impl Debug for InnerSubscriber {
    fn fmt(&self, f: &mut Formatter) -> ::std::fmt::Result {
        write!(f, "InnerSubscriber[ id: {} ]", self.id)
    }
}

pub struct Callback<M: Send + Sync + 'static> {
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
        Self { inner: Arc::new(Mutex::new(InnerBus { subscribers: HashSet::new() })), current_id: Mutex::new(0) }
    }

    pub fn create_subscriber(&self) -> Subscriber {
        let mut current_id = self.current_id.lock().unwrap(); // TODO error handling

        let inner = Arc::new(InnerSubscriber::new(*current_id));

        let mut unlocked_bus = self.inner.lock().unwrap(); // TODO error handling
        unlocked_bus.subscribers.insert(Arc::clone(&inner));

        *current_id = *current_id + 1;

        Subscriber { inner }
    }

    pub fn publish<M: Send + Sync + 'static>(&self, message: M) {
        let arc = Arc::new(message);

        //TODO error handling remove this unwrap()
        for sub in &self.inner.lock().unwrap().subscribers {
            sub.receive(Arc::clone(&arc));
        }
    }
}

impl Subscriber {
    pub fn on_message<F, M>(&mut self, callback: F) where F: Fn(&M) + Send + 'static, M: Send + Sync + 'static {
        self.inner.on_message(Callback::new(callback))


        /*
        let mut unlocked_bus = self.inner_bus.lock().unwrap(); // TODO error handling

        // The arc count of the inner sub is 2 (one in the bus set and one in self.inner) in order
        // to modify the inner sub, we must return this to one in order to be able to unwrap the
        // arc. We've got a mutable borrow of self and just got a lock on the bus, we're all set for
        // some arc black magic, with no unsafe, please :D

        // remove the inner sub from the bus set, arc count = 1
        unlocked_bus.subscribers.remove(&self.inner);

        // clone the inner sub arc, arc count = 2
        let sub = Arc::clone(&self.inner);

        // replace the inner sub arc by a another one, arc count = 1
        // TODO find a way not to create this subscriber ? this creates a hashmap and spawns a thread...
        self.inner = Arc::new(InnerSubscriber::new(0));

        // now, the arc count is 1, so we can try_unwrap
        // TODO error handling remove unwrap
        self.inner = Arc::new({
            let mut sub = Arc::try_unwrap(sub).unwrap();
            sub.on_message(Callback::new(callback));
            sub
        });

        // Add back the inner sub to the bus set, arc count = 2
        unlocked_bus.subscribers.insert(Arc::clone(&self.inner));*/
    }
}


impl InnerSubscriber {
    fn new(id: u32) -> Self {
        let (tx, rx) = channel();
        let result = Self {
            id,
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
                    Ok(SubscriberTask::Run { type_id, payload, callback }) => {
                        callbacks.get(&type_id).map(|it| callback.call_tuple((Arc::clone(it), payload)));
                    }
                    Err(_) => { /* sender is dead */break; }
                }
            }
        });
    }

    pub fn on_message<M: Send + Sync + 'static>(&self, callback: Callback<M>) {
        self.tx.send(SubscriberTask::RegisterCallback { callback: Box::new(callback), type_id: TypeId::of::<M>() }).unwrap()
    }

    fn receive<M: Send + Sync + 'static>(&self, message: Arc<M>) {
        self.tx.send(SubscriberTask::Run {
            type_id: TypeId::of::<M>(),
            payload: Box::new(message),
            callback: SendBoxFnOnce::new(|c: Arc<Box<Any>>, payload: Box<Any>| {
                let callback = c.downcast_ref::<Callback<M>>().unwrap();
                let message = payload.downcast_ref::<Arc<M>>().unwrap();
                callback.call(message);
            }
            ),
        }).unwrap() // TODO error handling
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
}


