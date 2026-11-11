//! A repository's git status on the host. The first event resets the status (`reset: true`)
//! and carries it whole; then differences. After a reconnect `Disconnected` comes first,
//! then the replacement subscription resets the status; after a daemon restart it opens the
//! root and repository again by their paths first. Show `state.complete`/`stale` as they are.
//!
//! Dropping the `Git` lets go of it at once, even while nothing changes and even while the
//! application has not read its events: the host ends its subscription (the last one stops the
//! repository's watches and git commands), nothing more is handed over, and a reconnection never
//! subscribes it again. Dropping only the receiver lets go at the next event.
use crate::delivery::{Delivered, Following, deliver, deliver_disconnected};
use crate::link::{Link, StreamItem};
use crate::{Client, ClientError, Resume};
use std::sync::mpsc::{Receiver, SyncSender, sync_channel};
use std::sync::{Arc, Mutex};
use tessel_host_protocol::git::{
    self, Changed, GitStats, Open, Opened, Refresh, Stats, Subscribe, Subscribed, Unsubscribe,
    Untracked, UntrackedList,
};
use tessel_host_protocol::tree::{self, RootOpen, RootOpened};
use tessel_host_protocol::{RawMessage, RelPath, RepoId, StreamId};

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum GitEvent {
    Changed(Changed),
    Disconnected,
}

struct Shared {
    client: Client,
    root_path: String,
    below: RelPath,
    repo: Mutex<RepoId>,
    events: SyncSender<GitEvent>,
    following: Mutex<Following>,
}

pub struct Git {
    shared: Arc<Shared>,
    pub opened: Opened,
}

impl Drop for Git {
    fn drop(&mut self) {
        self.shared.let_go();
    }
}

fn open_repo(client: &Client, root_path: &str, below: &RelPath) -> Result<Opened, ClientError> {
    let root: RootOpened = client.ask(
        tree::kind::ROOT_OPEN,
        &RootOpen {
            path: root_path.to_owned(),
        },
    )?;
    client.ask(
        git::kind::OPEN,
        &Open {
            root: root.root,
            path: below.clone(),
        },
    )
}

impl Git {
    /// The repository holding `below` under `root_path` (absolute, on the host), subscribed.
    pub fn open(
        client: &Client,
        root_path: &str,
        below: &RelPath,
    ) -> Result<(Self, Receiver<GitEvent>), ClientError> {
        let opened = client.retrying(|| open_repo(client, root_path, below))?;
        let (events, receiver) = sync_channel(crate::link::STREAM_BUFFER);
        let shared = Arc::new(Shared {
            client: client.clone(),
            root_path: root_path.to_owned(),
            below: below.clone(),
            repo: Mutex::new(opened.repo.clone()),
            events,
            following: Mutex::new(Following::default()),
        });
        let generation = shared
            .following
            .lock()
            .unwrap()
            .begin()
            .expect("nothing let go of a repository being opened");
        shared.subscribe(&client.link()?, generation)?;
        client.register(Arc::downgrade(&shared) as std::sync::Weak<dyn Resume>);
        Ok((Self { shared, opened }, receiver))
    }

    fn repo(&self) -> RepoId {
        self.shared.repo.lock().unwrap().clone()
    }

    pub fn refresh(&self) -> Result<(), ClientError> {
        self.shared
            .client
            .retrying(|| {
                self.shared
                    .client
                    .ask::<serde_json::Value>(git::kind::REFRESH, &Refresh { repo: self.repo() })
            })
            .map(|_| ())
    }

    pub fn untracked(&self, path: &RelPath) -> Result<UntrackedList, ClientError> {
        self.shared.client.retrying(|| {
            self.shared.client.ask(
                git::kind::UNTRACKED,
                &Untracked {
                    repo: self.repo(),
                    path: path.clone(),
                },
            )
        })
    }

    pub fn stats(&self) -> Result<GitStats, ClientError> {
        self.shared
            .client
            .ask(git::kind::STATS, &Stats { repo: self.repo() })
    }
}

impl Shared {
    /// Subscribes as `generation`. Kept and registered under the following lock: letting go
    /// after this ends this stream (and its follower's wait), letting go before it means it is
    /// never followed, and the host is told here to end it.
    fn subscribe(self: &Arc<Self>, link: &Arc<Link>, generation: u64) -> Result<(), ClientError> {
        let repo = self.repo.lock().unwrap().clone();
        let subscribed: Subscribed = link
            .request(
                RawMessage::of(git::kind::SUBSCRIBE, &Subscribe { repo }),
                self.client.options().request_timeout,
            )?
            .body_as()
            .map_err(ClientError::Refused)?;
        let receiver = {
            let mut following = self.following.lock().unwrap();
            if !following.keep(generation, link, &subscribed.stream) {
                drop(following);
                link.unregister(&subscribed.stream);
                let _ = link.request(
                    RawMessage::of(
                        git::kind::UNSUBSCRIBE,
                        &Unsubscribe {
                            stream: subscribed.stream,
                        },
                    ),
                    self.client.options().request_timeout,
                );
                return Ok(());
            }
            link.register(&subscribed.stream)
        };
        self.follow(link, subscribed.stream, receiver, generation);
        Ok(())
    }

    /// Hands the stream's events over in order, acknowledging each once handed over.
    fn follow(
        self: &Arc<Self>,
        link: &Arc<Link>,
        stream: StreamId,
        receiver: Receiver<StreamItem>,
        generation: u64,
    ) {
        let me = self.clone();
        let link = link.clone();
        std::thread::spawn(move || {
            for StreamItem { seq, message } in receiver {
                if message.kind == git::kind::CHANGED
                    && let Ok(change) = message.body_as::<Changed>()
                {
                    match deliver(
                        &me.following,
                        &me.events,
                        generation,
                        GitEvent::Changed(change),
                    ) {
                        Delivered::Sent | Delivered::Skipped => {}
                        // Let go, or a newer subscription follows the repository now.
                        Delivered::Stale => {
                            link.unregister(&stream);
                            return;
                        }
                        // The application dropped its receiver: let go of the repository.
                        Delivered::Closed => return me.let_go(),
                    }
                }
                link.ack(&stream, seq);
            }
            if !link.alive()
                && let Delivered::Closed = deliver_disconnected(
                    &me.following,
                    &me.events,
                    generation,
                    GitEvent::Disconnected,
                )
            {
                me.let_go();
            }
        });
    }

    /// The application let go: the follower stops at once (its stream is unregistered here),
    /// the repository is never subscribed again, and the host is asked to end the subscription
    /// from a thread of its own (never waiting for it here). Idempotent.
    fn let_go(&self) {
        let Some(Some((link, stream))) = self.following.lock().unwrap().abandon() else {
            return;
        };
        link.unregister(&stream);
        let timeout = self.client.options().request_timeout;
        std::thread::spawn(move || {
            let _ = link.request(
                RawMessage::of(git::kind::UNSUBSCRIBE, &Unsubscribe { stream }),
                timeout,
            );
        });
    }
}

impl Resume for Shared {
    fn resume(self: Arc<Self>, client: &Client, same_epoch: bool) {
        let Some(generation) = self.following.lock().unwrap().begin() else {
            return;
        };
        let client = client.clone();
        let Ok(link) = client.link() else { return };
        std::thread::spawn(move || self.reopen(&client, link, generation, same_epoch));
    }
}

impl Shared {
    fn reopen(
        self: Arc<Self>,
        client: &Client,
        link: Arc<Link>,
        generation: u64,
        same_epoch: bool,
    ) {
        match deliver_disconnected(
            &self.following,
            &self.events,
            generation,
            GitEvent::Disconnected,
        ) {
            Delivered::Sent | Delivered::Skipped => {}
            Delivered::Stale => return,
            Delivered::Closed => return self.let_go(),
        }
        if !same_epoch {
            let opened = (|| -> Result<Opened, ClientError> {
                let root: RootOpened = link
                    .request(
                        RawMessage::of(
                            tree::kind::ROOT_OPEN,
                            &RootOpen {
                                path: self.root_path.clone(),
                            },
                        ),
                        client.options().request_timeout,
                    )?
                    .body_as()
                    .map_err(ClientError::Refused)?;
                link.request(
                    RawMessage::of(
                        git::kind::OPEN,
                        &Open {
                            root: root.root,
                            path: self.below.clone(),
                        },
                    ),
                    client.options().request_timeout,
                )?
                .body_as()
                .map_err(ClientError::Refused)
            })();
            let Ok(opened) = opened else { return };
            let following = self.following.lock().unwrap();
            if !following.admits(generation) {
                return;
            }
            *self.repo.lock().unwrap() = opened.repo;
        }
        let _ = self.subscribe(&link, generation);
    }
}
