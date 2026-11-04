//! What one connection sends, in two queues: replies first, then stream events. Event volume
//! is bounded by each attachment's acknowledgment window, replies by the requests in flight,
//! so neither grows without bound; one writer thread per connection drains them.
use std::collections::VecDeque;
use std::io::Write;
use std::os::unix::net::UnixStream;
use std::sync::{Condvar, Mutex};

#[derive(Default)]
struct Queues {
    control: VecDeque<Vec<u8>>,
    events: VecDeque<Vec<u8>>,
    closed: bool,
    /// No more frames: the writer sends what is queued, then stops.
    ending: bool,
}

#[derive(Default)]
pub struct Outbound {
    queues: Mutex<Queues>,
    ready: Condvar,
}

impl Outbound {
    pub fn control(&self, frame: Vec<u8>) {
        self.push(frame, true);
    }

    pub fn event(&self, frame: Vec<u8>) {
        self.push(frame, false);
    }

    fn push(&self, frame: Vec<u8>, control: bool) {
        let Ok(mut queues) = self.queues.lock() else {
            return;
        };
        if queues.closed {
            return;
        }
        if control {
            queues.control.push_back(frame);
        } else {
            queues.events.push_back(frame);
        }
        self.ready.notify_one();
    }

    pub fn close(&self) {
        if let Ok(mut queues) = self.queues.lock() {
            queues.closed = true;
            queues.control.clear();
            queues.events.clear();
        }
        self.ready.notify_all();
    }

    /// The connection ends: what is queued (a last refusal) is still written.
    pub fn end(&self) {
        if let Ok(mut queues) = self.queues.lock() {
            queues.ending = true;
        }
        self.ready.notify_all();
    }

    pub fn is_closed(&self) -> bool {
        self.queues.lock().map(|q| q.closed).unwrap_or(true)
    }

    /// The writer thread: until closed or the socket fails.
    pub fn drain(&self, mut socket: UnixStream) {
        loop {
            let frame = {
                let Ok(mut queues) = self.queues.lock() else {
                    return;
                };
                loop {
                    if queues.closed {
                        return;
                    }
                    if let Some(frame) = queues
                        .control
                        .pop_front()
                        .or_else(|| queues.events.pop_front())
                    {
                        break frame;
                    }
                    if queues.ending {
                        return;
                    }
                    queues = match self.ready.wait(queues) {
                        Ok(queues) => queues,
                        Err(_) => return,
                    };
                }
            };
            if socket.write_all(&frame).is_err() {
                self.close();
                return;
            }
        }
    }
}
