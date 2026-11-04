//! A repository's git status on the host. The first event resets the status (`reset: true`)
//! and carries it whole; then differences. After a reconnect the session subscribes again and
//! the next event resets the status again; after a daemon restart it opens the root and the
//! repository again by their paths first. Show `state.complete`/`stale` as they are.
use crate::link::{Link, StreamItem};
use crate::{Client, ClientError, Resume};
use std::sync::mpsc::{Receiver, SyncSender, sync_channel};
use std::sync::{Arc, Mutex};
use tessel_host_protocol::git::{
    self, Changed, GitStats, Open, Opened, Refresh, Stats, Subscribe, Subscribed, Untracked,
    UntrackedList,
};
use tessel_host_protocol::tree::{self, RootOpen, RootOpened};
use tessel_host_protocol::{RelPath, RepoId, StreamId};

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
}

pub struct Git {
    shared: Arc<Shared>,
    pub opened: Opened,
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
        });
        shared.subscribe(&client.link()?)?;
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
    fn subscribe(self: &Arc<Self>, link: &Arc<Link>) -> Result<(), ClientError> {
        let repo = self.repo.lock().unwrap().clone();
        let subscribed: Subscribed = link
            .request(
                tessel_host_protocol::RawMessage::of(git::kind::SUBSCRIBE, &Subscribe { repo }),
                self.client.options().request_timeout,
            )?
            .body_as()
            .map_err(ClientError::Refused)?;
        self.follow(link, subscribed.stream);
        Ok(())
    }

    fn follow(self: &Arc<Self>, link: &Arc<Link>, stream: StreamId) {
        let receiver = link.register(&stream);
        let me = self.clone();
        let link = link.clone();
        std::thread::spawn(move || {
            for StreamItem { seq, message } in receiver {
                if message.kind == git::kind::CHANGED
                    && let Ok(change) = message.body_as::<Changed>()
                    && me.events.send(GitEvent::Changed(change)).is_err()
                {
                    break;
                }
                link.ack(&stream, seq);
            }
            if !link.alive() {
                let _ = me.events.send(GitEvent::Disconnected);
            }
        });
    }
}

impl Resume for Shared {
    fn resume(self: Arc<Self>, client: &Client, same_epoch: bool) {
        if !same_epoch {
            match open_repo(client, &self.root_path, &self.below) {
                Ok(opened) => *self.repo.lock().unwrap() = opened.repo,
                Err(_) => return,
            }
        }
        if let Ok(link) = client.link() {
            let _ = self.subscribe(&link);
        }
    }
}
