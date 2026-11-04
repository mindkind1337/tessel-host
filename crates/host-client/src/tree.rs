//! A file tree on the host. Expand the directories you show; apply `Changed` events of a
//! directory with a revision above its listing's. After a reconnect the tree subscribes again
//! and lists every expanded directory again (`Relisted`, from the host's cache when it kept
//! them); after a daemon restart it opens the root again by its path first.
use crate::link::{Link, StreamItem};
use crate::{Client, ClientError, Resume};
use std::collections::BTreeSet;
use std::sync::mpsc::{Receiver, SyncSender, sync_channel};
use std::sync::{Arc, Mutex};
use tessel_host_protocol::tree::{
    self, Changed, Collapse, Expand, Listing, Resynced, RootOpen, RootOpened, Stale, Stats,
    Subscribe, Subscribed, TreeStats,
};
use tessel_host_protocol::{RelPath, RootId, StreamId};

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TreeEvent {
    Changed(Changed),
    /// Expand this directory again (from the cache): what is shown may be out of date.
    Stale(Stale),
    Resynced(Resynced),
    /// A directory listed again after a reconnect: replace what you show for it.
    Relisted(Listing),
    Disconnected,
}

struct Shared {
    client: Client,
    path: String,
    root: Mutex<RootId>,
    expanded: Mutex<BTreeSet<RelPath>>,
    events: SyncSender<TreeEvent>,
}

pub struct Tree {
    shared: Arc<Shared>,
    pub label: String,
}

impl Tree {
    /// Opens `path` (absolute, on the host) as a root and subscribes to its changes.
    pub fn open(client: &Client, path: &str) -> Result<(Self, Receiver<TreeEvent>), ClientError> {
        let opened: RootOpened = client.retrying(|| {
            client.ask(
                tree::kind::ROOT_OPEN,
                &RootOpen {
                    path: path.to_owned(),
                },
            )
        })?;
        let (events, receiver) = sync_channel(crate::link::STREAM_BUFFER);
        let shared = Arc::new(Shared {
            client: client.clone(),
            path: path.to_owned(),
            root: Mutex::new(opened.root),
            expanded: Mutex::new(BTreeSet::new()),
            events,
        });
        shared.subscribe(&client.link()?)?;
        client.register(Arc::downgrade(&shared) as std::sync::Weak<dyn Resume>);
        Ok((
            Self {
                shared,
                label: opened.label,
            },
            receiver,
        ))
    }

    pub fn root(&self) -> RootId {
        self.shared.root.lock().unwrap().clone()
    }

    pub fn expand(&self, path: &RelPath) -> Result<Listing, ClientError> {
        let listing: Listing = self.shared.client.retrying(|| {
            self.shared.client.ask(
                tree::kind::EXPAND,
                &Expand {
                    root: self.root(),
                    path: path.clone(),
                },
            )
        })?;
        self.shared.expanded.lock().unwrap().insert(path.clone());
        Ok(listing)
    }

    pub fn collapse(&self, path: &RelPath) -> Result<(), ClientError> {
        self.shared
            .expanded
            .lock()
            .unwrap()
            .retain(|p| !p.starts_with(path));
        self.shared
            .client
            .retrying(|| {
                self.shared.client.ask::<serde_json::Value>(
                    tree::kind::COLLAPSE,
                    &Collapse {
                        root: self.root(),
                        path: path.clone(),
                    },
                )
            })
            .map(|_| ())
    }

    pub fn stats(&self) -> Result<TreeStats, ClientError> {
        self.shared
            .client
            .ask(tree::kind::STATS, &Stats { root: self.root() })
    }
}

impl Shared {
    fn subscribe(self: &Arc<Self>, link: &Arc<Link>) -> Result<(), ClientError> {
        let root = self.root.lock().unwrap().clone();
        let subscribed: Subscribed = link
            .request(
                tessel_host_protocol::RawMessage::of(tree::kind::SUBSCRIBE, &Subscribe { root }),
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
                let event = match message.kind.as_str() {
                    tree::kind::CHANGED => message.body_as().ok().map(TreeEvent::Changed),
                    tree::kind::STALE => message.body_as().ok().map(TreeEvent::Stale),
                    tree::kind::RESYNCED => message.body_as().ok().map(TreeEvent::Resynced),
                    _ => None,
                };
                if let Some(event) = event
                    && me.events.send(event).is_err()
                {
                    break;
                }
                link.ack(&stream, seq);
            }
            if !link.alive() {
                let _ = me.events.send(TreeEvent::Disconnected);
            }
        });
    }
}

impl Resume for Shared {
    fn resume(self: Arc<Self>, client: &Client, same_epoch: bool) {
        if !same_epoch {
            // The root's id belonged to the old daemon: open it again by its path.
            match client.ask::<RootOpened>(
                tree::kind::ROOT_OPEN,
                &RootOpen {
                    path: self.path.clone(),
                },
            ) {
                Ok(opened) => *self.root.lock().unwrap() = opened.root,
                Err(_) => return,
            }
        }
        let Ok(link) = client.link() else { return };
        if self.subscribe(&link).is_err() {
            return;
        }
        let root = self.root.lock().unwrap().clone();
        let expanded: Vec<RelPath> = self.expanded.lock().unwrap().iter().cloned().collect();
        for path in expanded {
            if let Ok(listing) = client.ask::<Listing>(
                tree::kind::EXPAND,
                &Expand {
                    root: root.clone(),
                    path: path.clone(),
                },
            ) {
                let _ = self.events.send(TreeEvent::Relisted(listing));
            } else {
                self.expanded.lock().unwrap().remove(&path);
            }
        }
    }
}
