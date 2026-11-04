//! Gate 5: the editor's file operations over the real bridge, against the real file system.
#![cfg(target_os = "linux")]
mod common;

use common::*;
use sha2::{Digest as _, Sha256};
use std::os::unix::fs::{PermissionsExt, symlink};
use std::path::Path;
use std::time::{Duration, Instant};
use tessel_host_protocol::file::*;
use tessel_host_protocol::tree::{Expand, Listing, RootOpen, RootOpened};
use tessel_host_protocol::*;

fn digest(bytes: &[u8]) -> Digest {
    let mut out = [0u8; 32];
    out.copy_from_slice(&Sha256::digest(bytes));
    Digest::from_bytes(out)
}

fn rel(name: &str) -> RelPath {
    RelPath::from_components(name.split('/').map(|p| p.as_bytes().to_vec()).collect()).unwrap()
}

fn noise(len: usize, seed: u8) -> Vec<u8> {
    (0..len)
        .map(|i| (i as u8).wrapping_mul(31).wrapping_add(seed) ^ (i >> 7) as u8)
        .collect()
}

struct Editor {
    client: Client,
    root: RootId,
}

impl Editor {
    fn open(home: &mut Home, dir: &Path) -> Self {
        let mut client = home.client(Budgets::SLOW_HOST);
        let opened: RootOpened = client.ask(
            tessel_host_protocol::tree::kind::ROOT_OPEN,
            &RootOpen {
                path: dir.to_string_lossy().into_owned(),
            },
        );
        Self {
            client,
            root: opened.root,
        }
    }

    /// The whole file, chunk by chunk, with the version it was read at.
    fn read_all(&mut self, name: &str) -> Result<(Vec<u8>, FileVersion), WireError> {
        let mut bytes = Vec::new();
        let mut first: Option<FileVersion> = None;
        loop {
            let chunk: ReadChunk = self
                .client
                .request(
                    kind::READ,
                    &Read {
                        root: self.root.clone(),
                        path: rel(name),
                        offset: bytes.len() as u64,
                        length: 65536,
                    },
                )?
                .body_as()
                .unwrap();
            assert_eq!(
                *first.get_or_insert(chunk.version.clone()),
                chunk.version,
                "one version for the whole read"
            );
            bytes.extend_from_slice(chunk.data.as_bytes());
            if chunk.eof {
                return Ok((bytes, first.unwrap()));
            }
        }
    }

    fn begin(&mut self, name: &str, base: Option<Base>, size: u64) -> Result<UploadId, WireError> {
        self.client
            .request(
                kind::WRITE_BEGIN,
                &WriteBegin {
                    root: self.root.clone(),
                    path: rel(name),
                    base,
                    size,
                },
            )
            .map(|ok| ok.body_as::<Upload>().unwrap().upload)
    }

    fn send(&mut self, upload: &UploadId, bytes: &[u8], from: usize) -> Result<(), WireError> {
        let mut offset = from;
        for piece in bytes[from..].chunks(60_000) {
            self.client.request(
                kind::WRITE_CHUNK,
                &WriteChunk {
                    upload: upload.clone(),
                    offset: offset as u64,
                    data: Chunk::new(piece.to_vec()).unwrap(),
                },
            )?;
            offset += piece.len();
        }
        Ok(())
    }

    fn commit(&mut self, upload: &UploadId) -> Result<Written, WireError> {
        self.client
            .request(
                kind::WRITE_COMMIT,
                &WriteCommit {
                    upload: upload.clone(),
                },
            )
            .map(|ok| ok.body_as().unwrap())
    }

    fn save(&mut self, name: &str, base: Option<Base>, bytes: &[u8]) -> Result<Written, WireError> {
        let upload = self.begin(name, base, bytes.len() as u64)?;
        self.send(&upload, bytes, 0)?;
        self.commit(&upload)
    }

    fn names(&mut self, dir: &str) -> Vec<String> {
        let path = if dir.is_empty() {
            RelPath::root()
        } else {
            rel(dir)
        };
        let listing: Listing = self.client.ask(
            tessel_host_protocol::tree::kind::EXPAND,
            &Expand {
                root: self.root.clone(),
                path,
            },
        );
        listing.entries.iter().map(|e| e.label.clone()).collect()
    }
}

fn temporaries(names: &[String]) -> Vec<&String> {
    names
        .iter()
        .filter(|n| n.starts_with(".tessel-upload-"))
        .collect()
}

fn project(home: &Home) -> std::path::PathBuf {
    let dir = home.path.join("project");
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

#[test]
fn original_bytes_in_chunks_and_an_atomic_save_that_keeps_the_mode() {
    let mut home = Home::new("save");
    let dir = project(&home);
    let original = noise(200_000, 7);
    std::fs::write(dir.join("data.bin"), &original).unwrap();
    std::fs::set_permissions(dir.join("data.bin"), std::fs::Permissions::from_mode(0o640)).unwrap();
    let mut editor = Editor::open(&mut home, &dir);
    let (read, version) = editor.read_all("data.bin").unwrap();
    assert_eq!(
        read, original,
        "the original bytes, NULs and invalid UTF-8 included"
    );

    let updated = noise(300_000, 9);
    let written = editor
        .save(
            "data.bin",
            Some(Base {
                version,
                digest: digest(&original),
            }),
            &updated,
        )
        .unwrap();
    assert_eq!(written.digest, digest(&updated));
    assert_eq!(std::fs::read(dir.join("data.bin")).unwrap(), updated);
    assert_eq!(
        std::fs::metadata(dir.join("data.bin"))
            .unwrap()
            .permissions()
            .mode()
            & 0o777,
        0o640
    );
    assert!(temporaries(&editor.names("")).is_empty());
    let info: Info = editor.client.ask(
        kind::STAT,
        &Stat {
            root: editor.root.clone(),
            path: rel("data.bin"),
        },
    );
    assert_eq!((info.version, info.size), (written.version, 300_000));
}

#[test]
fn a_file_changed_between_the_read_and_the_save_is_a_conflict() {
    let mut home = Home::new("conflict");
    let dir = project(&home);
    std::fs::write(dir.join("notes.txt"), b"mine\n").unwrap();
    let mut editor = Editor::open(&mut home, &dir);
    let (read, version) = editor.read_all("notes.txt").unwrap();
    let base = Base {
        version: version.clone(),
        digest: digest(&read),
    };

    // Someone else writes it after the read: refused when the save starts.
    std::thread::sleep(Duration::from_millis(20));
    std::fs::write(dir.join("notes.txt"), b"theirs\n").unwrap();
    let refused = editor
        .save("notes.txt", Some(base.clone()), b"mine, edited\n")
        .unwrap_err();
    assert_eq!(refused.code, ErrorCode::Conflict);
    assert_eq!(std::fs::read(dir.join("notes.txt")).unwrap(), b"theirs\n");

    // And while the save is being sent: refused at the commit.
    let (read, version) = editor.read_all("notes.txt").unwrap();
    let base = Base {
        version,
        digest: digest(&read),
    };
    let upload = editor.begin("notes.txt", Some(base), 12).unwrap();
    editor.send(&upload, b"mine, again\n", 0).unwrap();
    std::fs::write(dir.join("notes.txt"), b"theirs, again\n").unwrap();
    assert_eq!(
        editor.commit(&upload).unwrap_err().code,
        ErrorCode::Conflict
    );
    assert_eq!(
        std::fs::read(dir.join("notes.txt")).unwrap(),
        b"theirs, again\n"
    );
    assert!(temporaries(&editor.names("")).is_empty());

    // Touched but not changed: the same content, so the save goes through.
    let (read, version) = editor.read_all("notes.txt").unwrap();
    let file = std::fs::OpenOptions::new()
        .write(true)
        .open(dir.join("notes.txt"))
        .unwrap();
    file.set_modified(std::time::SystemTime::now() + Duration::from_secs(5))
        .unwrap();
    editor
        .save(
            "notes.txt",
            Some(Base {
                version,
                digest: digest(&read),
            }),
            b"mine at last\n",
        )
        .unwrap();
    assert_eq!(
        std::fs::read(dir.join("notes.txt")).unwrap(),
        b"mine at last\n"
    );
}

#[test]
fn a_new_file_never_replaces_one_created_meanwhile() {
    let mut home = Home::new("create");
    let dir = project(&home);
    let mut editor = Editor::open(&mut home, &dir);
    let upload = editor.begin("new.txt", None, 4).unwrap();
    editor.send(&upload, b"mine", 0).unwrap();
    std::fs::write(dir.join("new.txt"), b"theirs").unwrap();
    assert_eq!(
        editor.commit(&upload).unwrap_err().code,
        ErrorCode::Conflict
    );
    assert_eq!(std::fs::read(dir.join("new.txt")).unwrap(), b"theirs");
    let made = editor.save("other.txt", None, b"fresh").unwrap();
    assert_eq!(made.size, 5);
    assert_eq!(
        std::fs::metadata(dir.join("other.txt"))
            .unwrap()
            .permissions()
            .mode()
            & 0o777,
        0o644
    );
}

fn userns_tmpfs_works() -> bool {
    std::process::Command::new("unshare")
        .args(["-Urm", "true"])
        .stderr(std::process::Stdio::null())
        .status()
        .is_ok_and(|s| s.success())
}

#[test]
fn a_full_disk_leaves_neither_a_temporary_nor_a_truncated_file() {
    // Mid-write, as a full disk or a used-up quota answers (test knob: ENOSPC after 100 kB).
    let mut home = Home::new("full");
    home.env
        .push(("TESSEL_HOST_TEST_WRITE_LIMIT".into(), "100000".into()));
    let dir = project(&home);
    let original = noise(50_000, 3);
    std::fs::write(dir.join("doc.txt"), &original).unwrap();
    let mut editor = Editor::open(&mut home, &dir);
    let (read, version) = editor.read_all("doc.txt").unwrap();
    let refused = editor
        .save(
            "doc.txt",
            Some(Base {
                version,
                digest: digest(&read),
            }),
            &noise(300_000, 4),
        )
        .unwrap_err();
    assert_eq!(refused.code, ErrorCode::NoSpace);
    assert_eq!(
        std::fs::read(dir.join("doc.txt")).unwrap(),
        original,
        "never truncated"
    );
    assert!(temporaries(&editor.names("")).is_empty());

    // A real full file system: a 256 KiB tmpfs in a user namespace, where the system allows it.
    if !userns_tmpfs_works() {
        eprintln!("user namespaces unavailable here: real ENOSPC case skipped, injected case ran");
        return;
    }
    let mut home = Home::new("realfull");
    let small = home.path.join("small");
    std::fs::create_dir_all(&small).unwrap();
    home.wrap = Some(vec![
        "unshare".into(),
        "-Urm".into(),
        "sh".into(),
        "-c".into(),
        "mount -t tmpfs -o size=256k tmpfs \"$0\" && printf original > \"$0/doc.txt\" && exec \"$1\" connect --stdio".into(),
        small.to_string_lossy().into_owned(),
        EXE.into(),
    ]);
    let mut editor = Editor::open(&mut home, &small);
    let (read, version) = editor.read_all("doc.txt").unwrap();
    assert_eq!(read, b"original");
    let base = Base {
        version,
        digest: digest(&read),
    };
    let refused = editor
        .save("doc.txt", Some(base.clone()), &noise(400_000, 5))
        .unwrap_err();
    assert_eq!(refused.code, ErrorCode::NoSpace, "{refused}");
    assert_eq!(
        editor.read_all("doc.txt").unwrap().0,
        b"original",
        "never truncated"
    );
    assert!(
        temporaries(&editor.names("")).is_empty(),
        "no temporary file left"
    );
    // There is still room for a small save.
    editor.save("doc.txt", Some(base), b"fits").unwrap();
    assert_eq!(editor.read_all("doc.txt").unwrap().0, b"fits");
}

#[test]
fn an_interrupted_upload_is_removed_and_can_be_sent_again() {
    let mut home = Home::new("interrupted");
    home.env
        .push(("TESSEL_HOST_UPLOAD_IDLE_MS".into(), "400".into()));
    let dir = project(&home);
    std::fs::write(dir.join("draft.md"), b"v1").unwrap();
    let mut editor = Editor::open(&mut home, &dir);
    let (read, version) = editor.read_all("draft.md").unwrap();
    let base = Base {
        version,
        digest: digest(&read),
    };
    let edited = noise(150_000, 1);

    // The connection dies halfway: its upload goes; the buffer is sent again afterwards.
    let upload = editor
        .begin("draft.md", Some(base.clone()), edited.len() as u64)
        .unwrap();
    editor.send(&upload, &edited[..60_000], 0).unwrap();
    editor.client.kill();
    std::thread::sleep(Duration::from_millis(300));
    let mut editor = Editor::open(&mut home, &dir);
    assert!(temporaries(&editor.names("")).is_empty());
    assert_eq!(std::fs::read(dir.join("draft.md")).unwrap(), b"v1");
    editor.save("draft.md", Some(base), &edited).unwrap();
    assert_eq!(std::fs::read(dir.join("draft.md")).unwrap(), edited);

    // Left idle: removed too, and its next chunk is refused.
    let (read, version) = editor.read_all("draft.md").unwrap();
    let upload = editor
        .begin(
            "draft.md",
            Some(Base {
                version,
                digest: digest(&read),
            }),
            10,
        )
        .unwrap();
    std::thread::sleep(Duration::from_millis(1200));
    assert!(temporaries(&editor.names("")).is_empty());
    assert_eq!(
        editor.send(&upload, b"0123456789", 0).unwrap_err().code,
        ErrorCode::NotFound
    );
}

#[test]
fn a_daemon_that_died_mid_upload_leaves_nothing_after_the_next_start() {
    let mut home = Home::new("recover");
    let dir = project(&home);
    std::fs::write(dir.join("f"), b"x").unwrap();
    let mut editor = Editor::open(&mut home, &dir);
    let (read, version) = editor.read_all("f").unwrap();
    let upload = editor
        .begin(
            "f",
            Some(Base {
                version,
                digest: digest(&read),
            }),
            100,
        )
        .unwrap();
    editor.send(&upload, &[1u8; 50], 0).unwrap();
    let daemon = editor.client.host_info().pid;
    // SAFETY: the daemon this test started.
    unsafe { libc::kill(daemon as i32, libc::SIGKILL) };
    editor.client.kill();
    std::thread::sleep(Duration::from_millis(200));
    let left = || {
        std::fs::read_dir(&dir)
            .unwrap()
            .flatten()
            .filter(|e| {
                e.file_name()
                    .to_string_lossy()
                    .starts_with(".tessel-upload-")
            })
            .count()
    };
    assert_eq!(left(), 1, "the dead daemon's temporary file is there");
    let _editor = Editor::open(&mut home, &dir);
    let deadline = Instant::now() + Duration::from_secs(5);
    while left() > 0 {
        assert!(
            Instant::now() < deadline,
            "the next daemon did not remove it"
        );
        std::thread::sleep(Duration::from_millis(50));
    }
    assert_eq!(std::fs::read(dir.join("f")).unwrap(), b"x");
}

#[test]
fn compare_rename_remove_and_mkdir_never_replace_anything() {
    let mut home = Home::new("ops");
    let dir = project(&home);
    std::fs::write(dir.join("a.txt"), b"alpha").unwrap();
    std::fs::write(dir.join("b.txt"), b"beta").unwrap();
    std::fs::create_dir(dir.join("full")).unwrap();
    std::fs::write(dir.join("full/x"), b"x").unwrap();
    symlink("/etc/hostname", dir.join("link")).unwrap();
    let mut editor = Editor::open(&mut home, &dir);
    let root = editor.root.clone();

    let same: Compared = editor.client.ask(
        kind::COMPARE,
        &Compare {
            root: root.clone(),
            path: rel("a.txt"),
            digest: digest(b"alpha"),
        },
    );
    let other: Compared = editor.client.ask(
        kind::COMPARE,
        &Compare {
            root: root.clone(),
            path: rel("a.txt"),
            digest: digest(b"x"),
        },
    );
    assert!(same.same && !other.same);
    assert_eq!(same.digest, digest(b"alpha"));

    let code = |r: Result<RawMessage, WireError>| r.map(|_| ()).map_err(|e| e.code);
    let c = &mut editor.client;
    assert_eq!(
        code(c.request(
            kind::MKDIR,
            &Mkdir {
                root: root.clone(),
                path: rel("made")
            }
        )),
        Ok(())
    );
    assert_eq!(
        code(c.request(
            kind::MKDIR,
            &Mkdir {
                root: root.clone(),
                path: rel("made")
            }
        )),
        Err(ErrorCode::Conflict)
    );
    assert_eq!(
        code(c.request(
            kind::RENAME,
            &Rename {
                root: root.clone(),
                from: rel("a.txt"),
                to: rel("b.txt")
            }
        )),
        Err(ErrorCode::Conflict)
    );
    assert_eq!(
        (
            std::fs::read(dir.join("a.txt")).unwrap(),
            std::fs::read(dir.join("b.txt")).unwrap()
        ),
        (b"alpha".to_vec(), b"beta".to_vec())
    );
    assert_eq!(
        code(c.request(
            kind::RENAME,
            &Rename {
                root: root.clone(),
                from: rel("a.txt"),
                to: rel("made/a.txt")
            }
        )),
        Ok(())
    );
    assert!(dir.join("made/a.txt").exists());
    let stale = Remove {
        root: root.clone(),
        path: rel("b.txt"),
        version: Some(same.version.clone()),
    };
    assert_eq!(
        code(c.request(kind::REMOVE, &stale)),
        Err(ErrorCode::Conflict)
    );
    assert_eq!(
        code(c.request(
            kind::REMOVE,
            &Remove {
                root: root.clone(),
                path: rel("b.txt"),
                version: None
            }
        )),
        Ok(())
    );
    assert_eq!(
        code(c.request(
            kind::REMOVE,
            &Remove {
                root: root.clone(),
                path: rel("full"),
                version: None
            }
        )),
        Err(ErrorCode::Conflict)
    );
    // A link is never read through; a FIFO is refused without blocking.
    let through = c.request(
        kind::READ,
        &Read {
            root: root.clone(),
            path: rel("link"),
            offset: 0,
            length: 10,
        },
    );
    assert_eq!(code(through), Err(ErrorCode::BadRequest));
    let fifo = dir.join("pipe");
    let made = std::ffi::CString::new(fifo.to_string_lossy().as_bytes()).unwrap();
    // SAFETY: creating a FIFO for the test.
    assert_eq!(unsafe { libc::mkfifo(made.as_ptr(), 0o600) }, 0);
    let started = Instant::now();
    let blocked = c.request(
        kind::READ,
        &Read {
            root: root.clone(),
            path: rel("pipe"),
            offset: 0,
            length: 10,
        },
    );
    assert_eq!(code(blocked), Err(ErrorCode::BadRequest));
    assert!(started.elapsed() < Duration::from_secs(2));
}
