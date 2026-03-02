#[path = "support/env_lock.rs"]
mod support;

use std::path::Path;
use std::process::Command;
use std::time::{Duration, Instant};

const CHILD_FLAG: &str = "DELTA_BENCH_ENV_LOCK_CHILD";
const STARTED_PATH: &str = "DELTA_BENCH_ENV_LOCK_STARTED_PATH";
const ACQUIRED_PATH: &str = "DELTA_BENCH_ENV_LOCK_ACQUIRED_PATH";

#[test]
fn helper_child_attempts_lock() {
    if std::env::var_os(CHILD_FLAG).is_none() {
        return;
    }

    let started = std::env::var_os(STARTED_PATH).expect("started path env var");
    std::fs::write(&started, b"started").expect("write started marker");

    let _guard = support::env_lock();

    let acquired = std::env::var_os(ACQUIRED_PATH).expect("acquired path env var");
    std::fs::write(&acquired, b"acquired").expect("write acquired marker");
}

#[test]
fn env_lock_serializes_across_processes() {
    if std::env::var_os(CHILD_FLAG).is_some() {
        return;
    }

    let guard = support::env_lock();
    let temp = tempfile::tempdir().expect("tempdir");
    let started_path = temp.path().join("started");
    let acquired_path = temp.path().join("acquired");
    let current_exe = std::env::current_exe().expect("current exe");

    let mut child = Command::new(current_exe)
        .arg("--exact")
        .arg("helper_child_attempts_lock")
        .arg("--nocapture")
        .env(CHILD_FLAG, "1")
        .env(STARTED_PATH, started_path.as_os_str())
        .env(ACQUIRED_PATH, acquired_path.as_os_str())
        .spawn()
        .expect("spawn child");

    wait_for_file(&started_path, Duration::from_secs(5));
    std::thread::sleep(Duration::from_millis(200));
    assert!(
        !acquired_path.exists(),
        "child acquired env lock while parent still held it"
    );

    drop(guard);
    wait_for_file(&acquired_path, Duration::from_secs(5));

    let status = child.wait().expect("wait for child");
    assert!(status.success(), "child process failed: {status}");
}

fn wait_for_file(path: &Path, timeout: Duration) {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if path.exists() {
            return;
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    panic!("timed out waiting for {}", path.display());
}
