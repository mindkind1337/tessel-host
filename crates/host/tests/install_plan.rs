//! The install plan's commands, byte for byte, as other clients check their own port against:
//! `fixtures/install/plan.json` (version, nonce, and the eight strings of each architecture).
//! When `plan()` changes on purpose, replace the fixture's `plans` with what the failure prints.
use serde_json::{Value, json};
use std::path::PathBuf;
use tessel_host::install::{Arch, plan};

#[test]
fn the_fixture_is_exactly_what_plan_builds() {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../fixtures/install/plan.json");
    let text = std::fs::read_to_string(&path).expect("fixtures/install/plan.json");
    let fixture: Value = serde_json::from_str(&text).expect("valid JSON");
    let version = fixture["version"].as_str().expect("a version");
    let nonce = fixture["nonce"].as_str().expect("a nonce");
    let mut built = serde_json::Map::new();
    for arch in [Arch::X86_64, Arch::Aarch64] {
        let plan = plan(version, arch, nonce).expect("a plan for the fixture's version and nonce");
        built.insert(
            arch.name().to_owned(),
            json!({
                "probe": plan.probe,
                "upload": plan.upload,
                "digest": plan.digest,
                "activate": plan.activate,
                "abort": plan.abort,
                "installed": plan.installed,
                "version": plan.version,
                "connect": plan.connect,
            }),
        );
    }
    let built = Value::Object(built);
    assert_eq!(
        built,
        fixture["plans"],
        "fixtures/install/plan.json is out of date; its plans are now:\n{}",
        serde_json::to_string_pretty(&built).unwrap()
    );
}
