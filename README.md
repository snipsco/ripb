# `ripb`

A rust crate providing an implementation of a lock-free type-safe
in-process bus.

## Guarantees

- In order delivery : messages published on the bus are received in the
same order by a given subscriber. This is not guaranteed across multiple
subscribers. IE if you send Messages m1 and m2 on a bus with subscribers
s1 and s2, both s1 and s2 will receive the messages in the order m1, m2,
 but s1 may receive m2 before s2 has received m1.


## Implementation

Current implementation uses [`crossbeam-channel`]s and a fixed number of
threads. [`Any`] and [`TypeId`] are used to to be able to expose a
type-safe api.

