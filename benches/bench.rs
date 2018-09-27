#[macro_use]
extern crate bencher;
extern crate ripb;

use bencher::Bencher;
use ripb::{Bus, Subscriber};
use std::sync::mpsc::channel;

fn create_bus(bench: &mut Bencher) {
    bench.iter(|| {
        let _ = Bus::new();
    })
}

fn register_callback_on_single_subscriber(bench: &mut Bencher) {
    let bus = Bus::new();
    let sub = bus.create_subscriber();
    bench.iter(move || {
        sub.on_message(|_: &()| {})
    })
}

fn create_10_subscribers(bench: &mut Bencher) {
    create_n_subscribers(bench, 10)
}

fn create_100_subscribers(bench: &mut Bencher) {
    create_n_subscribers(bench, 100)
}

fn create_1000_subscribers(bench: &mut Bencher) {
    create_n_subscribers(bench, 1000)
}

fn create_n_subscribers(bench: &mut Bencher, n: u32) {
    let bus = Bus::new();
    bench.iter(|| {
        let _: Vec<Subscriber> = (0..n).map(|_| bus.create_subscriber()).collect();
    })
}

fn send_m_messages_to_n_subscribers(bench: &mut Bencher, m:u32, n: u32) {
    let bus = Bus::new();
    let subs: Vec<Subscriber> = (0..n).map(|_| bus.create_subscriber()).collect();
    let (tx, rx) = channel();
    for sub in &subs {
        let _tx = tx.clone();
        sub.on_message(move |_: &()| _tx.send(()).expect("could not send message"));
    }
    bench.iter(|| {
        for _ in 0..m { bus.publish(()) }
        for _ in 0..m*n { rx.recv_timeout(std::time::Duration::from_secs(1)).expect("could not receive message") }
    })
}

fn send_1_message_to_1_subscriber(bench: &mut Bencher) {
    send_m_messages_to_n_subscribers(bench, 1, 1)
}

fn send_1_message_to_10_subscribers(bench: &mut Bencher) {
    send_m_messages_to_n_subscribers(bench, 1, 10)
}

fn send_1_message_to_100_subscribers(bench: &mut Bencher) {
    send_m_messages_to_n_subscribers(bench, 1, 100)
}

fn send_1_message_to_1000_subscribers(bench: &mut Bencher) {
    send_m_messages_to_n_subscribers(bench, 1, 1000)
}

fn send_10_messages_to_1_subscriber(bench: &mut Bencher) {
    send_m_messages_to_n_subscribers(bench, 10, 1)
}

fn send_10_messages_to_10_subscribers(bench: &mut Bencher) {
    send_m_messages_to_n_subscribers(bench, 10, 10)
}

fn send_10_messages_to_100_subscribers(bench: &mut Bencher) {
    send_m_messages_to_n_subscribers(bench, 10, 100)
}

fn send_10_messages_to_1000_subscribers(bench: &mut Bencher) {
    send_m_messages_to_n_subscribers(bench, 10, 1000)
}

fn send_100_messages_to_1_subscriber(bench: &mut Bencher) {
    send_m_messages_to_n_subscribers(bench, 100, 1)
}

fn send_100_messages_to_10_subscribers(bench: &mut Bencher) {
    send_m_messages_to_n_subscribers(bench, 100, 10)
}

fn send_100_messages_to_100_subscribers(bench: &mut Bencher) {
    send_m_messages_to_n_subscribers(bench, 100, 100)
}

fn send_100_messages_to_1000_subscribers(bench: &mut Bencher) {
    send_m_messages_to_n_subscribers(bench, 100, 1000)
}

fn send_1000_messages_to_1_subscriber(bench: &mut Bencher) {
    send_m_messages_to_n_subscribers(bench, 1000, 1)
}

fn send_1000_messages_to_10_subscribers(bench: &mut Bencher) {
    send_m_messages_to_n_subscribers(bench, 1000, 10)
}

fn send_1000_messages_to_100_subscribers(bench: &mut Bencher) {
    send_m_messages_to_n_subscribers(bench, 1000, 100)
}

fn send_1000_messages_to_1000_subscribers(bench: &mut Bencher) {
    send_m_messages_to_n_subscribers(bench, 1000, 1000)
}

benchmark_group!(benches,
    create_bus,
    create_10_subscribers,
    create_100_subscribers,
    create_1000_subscribers,
    register_callback_on_single_subscriber,
    send_1_message_to_1_subscriber,
    send_1_message_to_10_subscribers,
    send_1_message_to_100_subscribers,
    send_1_message_to_1000_subscribers,
    send_10_messages_to_1_subscriber,
    send_10_messages_to_10_subscribers,
    send_10_messages_to_100_subscribers,
    send_10_messages_to_1000_subscribers,
    send_100_messages_to_1_subscriber,
    send_100_messages_to_10_subscribers,
    send_100_messages_to_100_subscribers,
    send_100_messages_to_1000_subscribers,
    send_1000_messages_to_1_subscriber,
    send_1000_messages_to_10_subscribers,
    send_1000_messages_to_100_subscribers,
    //send_1000_messages_to_1000_subscribers,
);
benchmark_main!(benches);
