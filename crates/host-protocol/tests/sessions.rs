//! The cross-client fixtures (`fixtures/sessions/*.jsonl`, recorded from real sessions): every
//! frame decodes and checks as its side's frame, request identifiers go up, every request gets
//! exactly one reply, and each stream's events are numbered 1, 2, 3… with acknowledgments only
//! for events sent. A client in another language can replay the same files.
use serde_json::Value;
use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use tessel_host_protocol::*;

#[test]
fn every_recorded_session_follows_the_protocol() {
    let dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../fixtures/sessions");
    let mut files: Vec<PathBuf> = std::fs::read_dir(&dir)
        .unwrap()
        .flatten()
        .map(|e| e.path())
        .filter(|p| p.extension().is_some_and(|x| x == "jsonl"))
        .collect();
    files.sort();
    assert!(files.len() >= 4, "fixtures missing in {}", dir.display());
    for file in files {
        let name = file.file_name().unwrap().to_string_lossy().into_owned();
        let mut last_request = 0u64;
        let mut waiting: HashSet<u64> = HashSet::new();
        let mut answered: HashSet<u64> = HashSet::new();
        let mut streams: HashMap<String, u64> = HashMap::new();
        let mut kinds: HashSet<String> = HashSet::new();
        for (number, line) in std::fs::read_to_string(&file).unwrap().lines().enumerate() {
            let at = format!("{name}:{}", number + 1);
            let entry: Value = serde_json::from_str(line).unwrap();
            let payload = serde_json::to_vec(&entry["frame"]).unwrap();
            match entry["from"].as_str() {
                Some("client") => {
                    let frame = decode_payload::<ClientFrame>(&payload)
                        .unwrap_or_else(|e| panic!("{at}: {e}"));
                    if let Some(id) = frame.request_id {
                        assert!(id > last_request, "{at}: identifiers go up");
                        last_request = id;
                        waiting.insert(id);
                        kinds.insert(frame.message.unwrap().kind);
                    }
                    if let Some(ack) = frame.ack {
                        let sent = streams.get(ack.stream.as_str()).copied().unwrap_or(0);
                        assert!(
                            ack.seq <= sent,
                            "{at}: an acknowledgment of an event never sent"
                        );
                    }
                }
                Some("host") => {
                    let frame = decode_payload::<HostFrame>(&payload)
                        .unwrap_or_else(|e| panic!("{at}: {e}"));
                    if let Some(id) = frame.reply_to {
                        assert!(waiting.remove(&id), "{at}: a reply to nothing waiting");
                        assert!(answered.insert(id), "{at}: two replies");
                    }
                    if let Some(stream) = frame.stream {
                        let next = streams.entry(stream.id.as_str().to_owned()).or_insert(0);
                        assert_eq!(
                            stream.seq,
                            *next + 1,
                            "{at}: stream events numbered without a gap"
                        );
                        *next = stream.seq;
                    }
                }
                other => panic!("{at}: unknown side {other:?}"),
            }
        }
        assert!(
            waiting.is_empty(),
            "{name}: requests left without a reply: {waiting:?}"
        );
        assert!(kinds.contains("hello"), "{name}: starts with hello");
    }
}
