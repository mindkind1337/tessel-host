//! A file tree on the host. Expand the directories you show; apply `Changed` events of a
//! directory with a revision above its listing's. After a reconnect the tree says
//! `Disconnected`, subscribes again and lists every expanded directory again (`Relisted`, from
//! the host's cache when it kept them); after a daemon restart it opens the root again by its
//! path first.
//!
//! Dropping the `Tree` lets go of it at once, even while nothing changes and even while the
//! application has not read its events: its subscription ends on the host, nothing more is
//! handed over, a reconnection never subscribes it again, and the directories it expanded are
//! folded on this connection unless another tree of this client still holds them (a tree
//! opened again on the same root keeps them, whichever goes first). Dropping only the receiver
//! lets go at the next event; what the tree expands after that is folded when it is dropped.
use crate::delivery::{Delivered, Following, deliver, deliver_disconnected};
use crate::link::{Link, StreamItem};
use crate::{Client, ClientError, Resume};
use std::collections::{BTreeSet, HashMap};
use std::sync::mpsc::{Receiver, SyncSender, sync_channel};
use std::sync::{Arc, Mutex};
use tessel_host_protocol::tree::{
    self, Changed, Collapse, Expand, Listing, Resynced, RootOpen, RootOpened, Stale, Stats,
    Subscribe, Subscribed, TreeStats, Unsubscribe,
};
use tessel_host_protocol::{RawMessage, RelPath, RootId, StreamId};

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

/// The directories this client's trees hold on the host. The host keeps directories per
/// connection, not per tree, and folds a whole subtree at once: a directory is folded only when
/// no live tree of its root holds it or anything below it. Deciding and writing the request
/// happen under one lock, for expansions as for folds, so the host gets them in the order they
/// were decided (it answers a connection's requests in order).
#[derive(Default)]
pub(crate) struct Folders {
    next: u64,
    trees: HashMap<u64, Held>,
    /// Let go of but perhaps still held on the host by this connection: kept until nothing at
    /// or below them is held any more, then folded.
    loose: HashMap<RootId, BTreeSet<RelPath>>,
}

struct Held {
    root: RootId,
    paths: BTreeSet<RelPath>,
}

impl Folders {
    fn add(&mut self, root: RootId) -> u64 {
        self.next += 1;
        self.trees.insert(
            self.next,
            Held {
                root,
                paths: BTreeSet::new(),
            },
        );
        self.next
    }

    #[cfg(test)]
    fn root(&self, tree: u64) -> Option<RootId> {
        self.trees.get(&tree).map(|h| h.root.clone())
    }

    /// Held before it is asked for: a fold decided meanwhile leaves it alone.
    fn hold(&mut self, tree: u64, path: &RelPath) -> Option<RootId> {
        let held = self.trees.get_mut(&tree)?;
        held.paths.insert(path.clone());
        Some(held.root.clone())
    }

    fn holds(&self, tree: u64, path: &RelPath) -> bool {
        self.trees
            .get(&tree)
            .is_some_and(|h| h.paths.contains(path))
    }

    fn paths(&self, tree: u64) -> Vec<RelPath> {
        self.trees
            .get(&tree)
            .map(|h| h.paths.iter().cloned().collect())
            .unwrap_or_default()
    }

    /// An expansion that failed: no longer held, but perhaps reached the host (a timeout).
    fn unhold(&mut self, tree: u64, path: &RelPath) {
        if let Some(held) = self.trees.get_mut(&tree)
            && held.paths.remove(path)
        {
            self.loose
                .entry(held.root.clone())
                .or_default()
                .insert(path.clone());
        }
    }

    /// The root opened again on a restarted daemon: what was held there is gone with it.
    fn reroot(&mut self, tree: u64, root: RootId) {
        if let Some(held) = self.trees.get_mut(&tree) {
            let old = std::mem::replace(&mut held.root, root);
            self.loose.remove(&old);
        }
    }

    /// Lets go of what `tree` holds at or below `below` (everything for `None`), and of the
    /// tree itself if `forget`. The directories to fold now, of that root.
    fn release(
        &mut self,
        tree: u64,
        below: Option<&RelPath>,
        forget: bool,
    ) -> Option<(RootId, Vec<RelPath>)> {
        let held = self.trees.get_mut(&tree)?;
        let root = held.root.clone();
        let released: Vec<RelPath> = match below {
            Some(top) => {
                let gone: Vec<RelPath> = held
                    .paths
                    .iter()
                    .filter(|p| p.starts_with(top))
                    .cloned()
                    .collect();
                for path in &gone {
                    held.paths.remove(path);
                }
                gone
            }
            None => std::mem::take(&mut held.paths).into_iter().collect(),
        };
        if forget {
            self.trees.remove(&tree);
        }
        let kept: Vec<RelPath> = self
            .trees
            .values()
            .filter(|h| h.root == root)
            .flat_map(|h| h.paths.iter().cloned())
            .collect();
        let loose = self.loose.entry(root.clone()).or_default();
        loose.extend(released);
        if let Some(top) = below {
            // A collapse asked by the application folds the folder itself too when it can.
            loose.insert(top.clone());
        }
        let free = loose
            .iter()
            .filter(|y| !kept.iter().any(|q| q.starts_with(y)))
            .cloned();
        let folds = topmost(free);
        loose.retain(|y| !folds.iter().any(|f| y.starts_with(f)));
        if loose.is_empty() {
            self.loose.remove(&root);
        }
        Some((root, folds))
    }
}

struct Shared {
    client: Client,
    path: String,
    /// This tree in the client's `Folders`.
    id: u64,
    root: Mutex<RootId>,
    events: SyncSender<TreeEvent>,
    following: Mutex<Following>,
}

pub struct Tree {
    shared: Arc<Shared>,
    pub label: String,
}

impl Drop for Tree {
    fn drop(&mut self) {
        self.shared.let_go(true);
    }
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
        let id = client.folders().add(opened.root.clone());
        let shared = Arc::new(Shared {
            client: client.clone(),
            path: path.to_owned(),
            id,
            root: Mutex::new(opened.root),
            events,
            following: Mutex::new(Following::default()),
        });
        let generation = shared
            .following
            .lock()
            .unwrap()
            .begin()
            .expect("nothing let go of a tree being opened");
        let subscribed = client
            .link()
            .and_then(|link| shared.subscribe(&link, generation));
        if let Err(error) = subscribed {
            client.folders().release(id, None, true);
            return Err(error);
        }
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
        self.shared.root()
    }

    pub fn expand(&self, path: &RelPath) -> Result<Listing, ClientError> {
        let shared = &self.shared;
        let was_held = shared.client.folders().holds(shared.id, path);
        let result = shared.client.retrying(|| shared.expand_once(path));
        if result.is_err() && !was_held {
            shared.client.folders().unhold(shared.id, path);
        }
        result
    }

    /// Stops showing `path` and what is below it. Folded on the host unless another tree of
    /// this client still holds it (then only what no tree holds any more is folded).
    pub fn collapse(&self, path: &RelPath) -> Result<(), ClientError> {
        let shared = &self.shared;
        let timeout = shared.client.options().request_timeout;
        let link;
        let replies = {
            let mut folders = shared.client.folders();
            link = shared.client.link();
            let Some((root, folds)) = folders.release(shared.id, Some(path), false) else {
                return Ok(());
            };
            // No live link: this connection's directories went with it.
            let Ok(link) = &link else { return Ok(()) };
            let mut replies = Vec::new();
            for fold in folds {
                replies.push(link.send(RawMessage::of(
                    tree::kind::COLLAPSE,
                    &Collapse {
                        root: root.clone(),
                        path: fold,
                    },
                ))?);
            }
            replies
        };
        for reply in replies {
            reply.wait(timeout)?;
        }
        Ok(())
    }

    pub fn stats(&self) -> Result<TreeStats, ClientError> {
        self.shared
            .client
            .ask(tree::kind::STATS, &Stats { root: self.root() })
    }
}

/// The paths with none of the others above them: folding those folds them all.
fn topmost(paths: impl IntoIterator<Item = RelPath>) -> Vec<RelPath> {
    let sorted: BTreeSet<RelPath> = paths.into_iter().collect();
    let mut kept: Vec<RelPath> = Vec::new();
    for path in sorted {
        // Sorted by components: whatever is below a path comes right after it.
        if kept.last().is_some_and(|top| path.starts_with(top)) {
            continue;
        }
        kept.push(path);
    }
    kept
}

impl Shared {
    fn root(&self) -> RootId {
        self.root.lock().unwrap().clone()
    }

    /// One expansion on the live link: held and written under the folders lock, so a fold
    /// decided meanwhile by another tree either comes first or leaves it alone.
    fn expand_once(&self, path: &RelPath) -> Result<Listing, ClientError> {
        let link = self.client.link()?;
        let reply = {
            let mut folders = self.client.folders();
            let root = folders
                .hold(self.id, path)
                .ok_or(ClientError::ConnectionLost)?;
            link.send(RawMessage::of(
                tree::kind::EXPAND,
                &Expand {
                    root,
                    path: path.clone(),
                },
            ))?
        };
        reply
            .wait(self.client.options().request_timeout)?
            .body_as()
            .map_err(ClientError::Refused)
    }

    /// Subscribes as `generation`. Kept and registered under the following lock: letting go
    /// after this ends this stream (and its follower's wait), letting go before it means it is
    /// never followed, and the host is told here to end it.
    fn subscribe(self: &Arc<Self>, link: &Arc<Link>, generation: u64) -> Result<(), ClientError> {
        let root = self.root();
        let subscribed: Subscribed = link
            .request(
                RawMessage::of(tree::kind::SUBSCRIBE, &Subscribe { root }),
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
                        tree::kind::UNSUBSCRIBE,
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

    /// Hands the stream's events over in order, acknowledging each once handed over (so a slow
    /// application holds the host back instead of piling events up here).
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
                let event = match message.kind.as_str() {
                    tree::kind::CHANGED => message.body_as().ok().map(TreeEvent::Changed),
                    tree::kind::STALE => message.body_as().ok().map(TreeEvent::Stale),
                    tree::kind::RESYNCED => message.body_as().ok().map(TreeEvent::Resynced),
                    _ => None,
                };
                if let Some(event) = event {
                    match deliver(&me.following, &me.events, generation, event) {
                        Delivered::Sent | Delivered::Skipped => {}
                        // Let go, or a newer subscription follows the tree now.
                        Delivered::Stale => {
                            link.unregister(&stream);
                            return;
                        }
                        // The application dropped its receiver: let go of the tree.
                        Delivered::Closed => return me.let_go(false),
                    }
                }
                link.ack(&stream, seq);
            }
            if !link.alive()
                && let Delivered::Closed = deliver_disconnected(
                    &me.following,
                    &me.events,
                    generation,
                    TreeEvent::Disconnected,
                )
            {
                me.let_go(false);
            }
        });
    }

    /// The application let go (`forget`: the tree itself is dropped). The follower stops at
    /// once: its stream is unregistered here on its own link, which ends its wait even while
    /// nothing changes, and one waiting for the application to read stops at its next try. The
    /// tree is never subscribed again. From a thread of its own (never waiting for the host
    /// here): the subscription is ended, and what the tree holds is let go of and folded where
    /// no other tree holds it. Idempotent: what was expanded after a first letting go (the
    /// receiver dropped, the tree kept) is folded by the next.
    fn let_go(&self, forget: bool) {
        let stream = self.following.lock().unwrap().abandon().flatten();
        if let Some((link, stream)) = &stream {
            link.unregister(stream);
        }
        let client = self.client.clone();
        let id = self.id;
        std::thread::spawn(move || {
            let timeout = client.options().request_timeout;
            if let Some((link, stream)) = stream {
                let _ = link.request(
                    RawMessage::of(tree::kind::UNSUBSCRIBE, &Unsubscribe { stream }),
                    timeout,
                );
            }
            let mut folders = client.folders();
            let link = client.link();
            let Some((root, folds)) = folders.release(id, None, forget) else {
                return;
            };
            // No live link: this connection's directories went with it.
            let Ok(link) = link else { return };
            for path in folds {
                // Written under the folders lock, not waited for.
                if link
                    .send(RawMessage::of(
                        tree::kind::COLLAPSE,
                        &Collapse {
                            root: root.clone(),
                            path,
                        },
                    ))
                    .is_err()
                {
                    return;
                }
            }
        });
    }

    /// After a reconnection, on a thread of its own (an application slow to read never holds
    /// back the other sessions): `Disconnected` first (the replaced follower may have stopped
    /// before saying it), then the root opened again if the daemon restarted, a new
    /// subscription, and each directory still held listed again.
    fn reopen(self: Arc<Self>, client: Client, link: Arc<Link>, generation: u64, same_epoch: bool) {
        match deliver_disconnected(
            &self.following,
            &self.events,
            generation,
            TreeEvent::Disconnected,
        ) {
            Delivered::Sent | Delivered::Skipped => {}
            Delivered::Stale => return,
            Delivered::Closed => return self.let_go(false),
        }
        if !same_epoch {
            // The root's id belonged to the old daemon: open it again by its path.
            match link
                .request(
                    RawMessage::of(
                        tree::kind::ROOT_OPEN,
                        &RootOpen {
                            path: self.path.clone(),
                        },
                    ),
                    client.options().request_timeout,
                )
                .and_then(|reply| reply.body_as::<RootOpened>().map_err(ClientError::Refused))
            {
                Ok(opened) => {
                    let following = self.following.lock().unwrap();
                    if !following.admits(generation) {
                        return;
                    }
                    let mut folders = client.folders();
                    *self.root.lock().unwrap() = opened.root.clone();
                    folders.reroot(self.id, opened.root);
                }
                Err(_) => return,
            }
        }
        if self.subscribe(&link, generation).is_err() {
            return;
        }
        let root = self.root();
        let timeout = client.options().request_timeout;
        let paths = client.folders().paths(self.id);
        for path in paths {
            if !self.following.lock().unwrap().admits(generation) {
                return;
            }
            // Asked only while this tree still holds it, under the folders lock: a drop
            // meanwhile either comes first (and it is not asked) or folds it after.
            let reply = {
                let folders = client.folders();
                if !folders.holds(self.id, &path) {
                    continue;
                }
                match link.send(RawMessage::of(
                    tree::kind::EXPAND,
                    &Expand {
                        root: root.clone(),
                        path: path.clone(),
                    },
                )) {
                    Ok(reply) => reply,
                    Err(_) => return,
                }
            };
            match reply
                .wait(timeout)
                .and_then(|r| r.body_as::<Listing>().map_err(ClientError::Refused))
            {
                Ok(listing) => match deliver(
                    &self.following,
                    &self.events,
                    generation,
                    TreeEvent::Relisted(listing),
                ) {
                    Delivered::Sent | Delivered::Skipped => {}
                    Delivered::Stale => return,
                    Delivered::Closed => return self.let_go(false),
                },
                Err(_) => client.folders().unhold(self.id, &path),
            }
        }
    }
}

impl Resume for Shared {
    fn resume(self: Arc<Self>, client: &Client, same_epoch: bool) {
        // The generation starts here, in the order of the links.
        let Some(generation) = self.following.lock().unwrap().begin() else {
            return;
        };
        let client = client.clone();
        let Ok(link) = client.link() else { return };
        std::thread::spawn(move || self.reopen(client, link, generation, same_epoch));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn path(parts: &[&str]) -> RelPath {
        RelPath::from_components(parts.iter().map(|p| p.as_bytes().to_vec()).collect()).unwrap()
    }

    fn root(n: u8) -> RootId {
        RootId::from_bytes([n; 16])
    }

    #[test]
    fn only_the_topmost_paths_are_folded() {
        let folds = topmost([
            path(&["src", "a"]),
            path(&["src"]),
            path(&["src0"]),
            path(&["docs", "x", "y"]),
            path(&["docs", "x"]),
            path(&["src", "b", "c"]),
        ]);
        assert_eq!(
            folds,
            vec![path(&["docs", "x"]), path(&["src"]), path(&["src0"])]
        );
        assert_eq!(
            topmost([path(&["a"]), RelPath::root()]),
            vec![RelPath::root()]
        );
        assert!(topmost(Vec::new()).is_empty());
    }

    #[test]
    fn a_tree_opened_again_on_the_same_root_keeps_what_it_holds() {
        let mut folders = Folders::default();
        let old = folders.add(root(1));
        folders.hold(old, &RelPath::root());
        folders.hold(old, &path(&["src"]));
        let new = folders.add(root(1));
        folders.hold(new, &RelPath::root());
        // The root stays expanded; src is not held by the new tree and can be folded.
        assert_eq!(
            folders.release(old, None, true),
            Some((root(1), vec![path(&["src"])]))
        );
        // The new one goes: everything, in one fold.
        assert_eq!(
            folders.release(new, None, true),
            Some((root(1), vec![RelPath::root()]))
        );
        assert!(folders.loose.is_empty());
        assert_eq!(folders.release(new, None, true), None, "gone");
    }

    #[test]
    fn a_folder_held_below_by_another_tree_waits_for_it() {
        let mut folders = Folders::default();
        let a = folders.add(root(1));
        let b = folders.add(root(1));
        let other_root = folders.add(root(2));
        folders.hold(a, &path(&["src"]));
        folders.hold(a, &path(&["docs"]));
        folders.hold(b, &path(&["src", "deep"]));
        folders.hold(other_root, &path(&["src"]));
        // src cannot be folded while b holds src/deep; docs can.
        assert_eq!(
            folders.release(a, None, true),
            Some((root(1), vec![path(&["docs"])]))
        );
        // b collapses src: now src goes, with what was below it.
        assert_eq!(
            folders.release(b, Some(&path(&["src"])), false),
            Some((root(1), vec![path(&["src"])]))
        );
        assert!(!folders.loose.contains_key(&root(1)));
        // Another root is never touched.
        assert!(folders.holds(other_root, &path(&["src"])));
    }

    #[test]
    fn a_failed_expansion_is_folded_with_the_rest() {
        let mut folders = Folders::default();
        let a = folders.add(root(1));
        folders.hold(a, &path(&["x"]));
        folders.unhold(a, &path(&["x"]));
        assert!(!folders.holds(a, &path(&["x"])));
        assert_eq!(
            folders.release(a, None, true),
            Some((root(1), vec![path(&["x"])]))
        );
    }

    #[test]
    fn a_restarted_daemon_forgets_what_was_loose_on_the_old_root() {
        let mut folders = Folders::default();
        let a = folders.add(root(1));
        let b = folders.add(root(1));
        folders.hold(a, &path(&["x"]));
        folders.hold(b, &path(&["x", "y"]));
        assert_eq!(folders.release(a, None, true), Some((root(1), Vec::new())));
        folders.reroot(b, root(9));
        assert!(folders.loose.is_empty());
        assert_eq!(folders.root(b), Some(root(9)));
    }
}
