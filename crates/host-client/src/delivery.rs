//! Hands a subscription's events (a `Tree`'s, a `Git`'s) to the application: only from the
//! current subscription, and to nobody once the application let go. Never blocks in a lock:
//! while the application has not read its events, it waits outside the lock and tries again,
//! so letting go ends that wait at the next try.
use crate::link::Link;
use std::sync::mpsc::{SyncSender, TrySendError};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tessel_host_protocol::StreamId;

/// Which subscription hands events over now, and whether the application let go.
#[derive(Default)]
pub(crate) struct Following {
    /// Bumped by each subscription (the first, then one per reconnection): only the follower
    /// of the current one hands events over.
    generation: u64,
    /// The current subscription's stream and the link it belongs to, ended when the
    /// application lets go.
    stream: Option<(Arc<Link>, StreamId)>,
    /// The last event handed over was `Disconnected`: a second one is skipped.
    disconnected: bool,
    /// The application let go (dropped the handle or its receiver). Under the same lock as the
    /// generation, so letting go and subscribing again never interleave: once set, nothing
    /// subscribes, nothing is current, nothing is handed over.
    abandoned: bool,
}

impl Following {
    /// A new subscription, unless the application let go.
    pub(crate) fn begin(&mut self) -> Option<u64> {
        if self.abandoned {
            return None;
        }
        self.generation += 1;
        Some(self.generation)
    }

    /// Whether `generation` may hand events over.
    pub(crate) fn admits(&self, generation: u64) -> bool {
        !self.abandoned && self.generation == generation
    }

    /// Keeps `stream` of `link` as the one to end on letting go, if `generation` is still
    /// current.
    pub(crate) fn keep(&mut self, generation: u64, link: &Arc<Link>, stream: &StreamId) -> bool {
        if !self.admits(generation) {
            return false;
        }
        self.stream = Some((link.clone(), stream.clone()));
        true
    }

    /// Lets go: nothing is current any more. `Some` the first time only, with the stream to
    /// end (and its link) if one was kept.
    pub(crate) fn abandon(&mut self) -> Option<Option<(Arc<Link>, StreamId)>> {
        if self.abandoned {
            return None;
        }
        self.abandoned = true;
        self.generation += 1;
        Some(self.stream.take())
    }
}

pub(crate) enum Delivered {
    Sent,
    /// A second `Disconnected` in a row: not handed over.
    Skipped,
    /// Not handed over: the application let go, or a newer subscription is current.
    Stale,
    /// The application dropped its receiver.
    Closed,
}

/// Hands `event` over if `generation` is current. The check and the hand-over happen under one
/// lock without ever blocking in it, so nothing is handed over once `abandon` returned; while
/// the application has not read its events, this waits outside the lock and tries again.
pub(crate) fn deliver<T>(
    following: &Mutex<Following>,
    events: &SyncSender<T>,
    generation: u64,
    event: T,
) -> Delivered {
    hand_over(following, events, generation, event, false)
}

/// Hands `Disconnected` over, once: a replaced follower may have said it already, or stopped
/// before (it no longer may once the next subscription began).
pub(crate) fn deliver_disconnected<T>(
    following: &Mutex<Following>,
    events: &SyncSender<T>,
    generation: u64,
    disconnected: T,
) -> Delivered {
    hand_over(following, events, generation, disconnected, true)
}

fn hand_over<T>(
    following: &Mutex<Following>,
    events: &SyncSender<T>,
    generation: u64,
    mut event: T,
    disconnect: bool,
) -> Delivered {
    let mut pause = Duration::from_millis(1);
    loop {
        {
            let mut following = following.lock().unwrap();
            if !following.admits(generation) {
                return Delivered::Stale;
            }
            if disconnect && following.disconnected {
                return Delivered::Skipped;
            }
            match events.try_send(event) {
                Ok(()) => {
                    following.disconnected = disconnect;
                    return Delivered::Sent;
                }
                Err(TrySendError::Full(back)) => event = back,
                Err(TrySendError::Disconnected(_)) => return Delivered::Closed,
            }
        }
        std::thread::sleep(pause);
        pause = (pause * 2).min(Duration::from_millis(20));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::mpsc::sync_channel;
    use std::time::Instant;

    #[test]
    fn letting_go_ends_a_wait_for_a_full_receiver() {
        let following = Arc::new(Mutex::new(Following::default()));
        let generation = following.lock().unwrap().begin().unwrap();
        let (events, receiver) = sync_channel(1);
        events.try_send(0).unwrap();
        let waiting = {
            let following = following.clone();
            std::thread::spawn(move || deliver(&following, &events, generation, 1))
        };
        std::thread::sleep(Duration::from_millis(50));
        let started = Instant::now();
        assert!(following.lock().unwrap().abandon().is_some());
        assert!(matches!(waiting.join().unwrap(), Delivered::Stale));
        assert!(started.elapsed() < Duration::from_secs(1));
        // Only what came before letting go.
        assert_eq!(receiver.try_iter().collect::<Vec<_>>(), vec![0]);
    }

    #[test]
    fn a_let_go_following_never_subscribes_again_and_tells_its_stream_once() {
        let mut following = Following::default();
        let first = following.begin().unwrap();
        let second = following.begin().unwrap();
        assert!(!following.admits(first), "a newer subscription is current");
        assert!(following.admits(second));
        assert!(matches!(following.abandon(), Some(None)));
        assert!(following.abandon().is_none(), "once");
        assert!(!following.admits(second));
        assert_eq!(following.begin(), None);
    }

    #[test]
    fn disconnected_is_handed_over_once_until_something_else_is() {
        let following = Mutex::new(Following::default());
        let old = following.lock().unwrap().begin().unwrap();
        let (events, receiver) = sync_channel(8);
        assert!(matches!(
            deliver_disconnected(&following, &events, old, "gone"),
            Delivered::Sent
        ));
        let new = following.lock().unwrap().begin().unwrap();
        assert!(matches!(
            deliver_disconnected(&following, &events, new, "gone"),
            Delivered::Skipped
        ));
        assert!(matches!(
            deliver(&following, &events, new, "listed"),
            Delivered::Sent
        ));
        // The old follower stopped before saying it: the next subscription says it.
        let newer = following.lock().unwrap().begin().unwrap();
        assert!(matches!(
            deliver_disconnected(&following, &events, old, "gone"),
            Delivered::Stale
        ));
        assert!(matches!(
            deliver_disconnected(&following, &events, newer, "gone"),
            Delivered::Sent
        ));
        assert_eq!(
            receiver.try_iter().collect::<Vec<_>>(),
            vec!["gone", "listed", "gone"]
        );
    }

    #[test]
    fn a_dropped_receiver_is_closed() {
        let following = Mutex::new(Following::default());
        let generation = following.lock().unwrap().begin().unwrap();
        let (events, receiver) = sync_channel::<u8>(1);
        drop(receiver);
        assert!(matches!(
            deliver(&following, &events, generation, 1),
            Delivered::Closed
        ));
    }
}
