use std::collections::hash_map::DefaultHasher;
use std::fs::{self, OpenOptions};
use std::hash::{Hash, Hasher};
use std::io::Write;
use std::path::PathBuf;
use std::thread;
use std::time::{Duration, Instant};

const ENV_LOCK_TIMEOUT: Duration = Duration::from_secs(300);
const ENV_LOCK_RETRY_INTERVAL: Duration = Duration::from_millis(20);

pub struct EnvLockGuard {
    lock_path: PathBuf,
}

pub fn env_lock() -> EnvLockGuard {
    let lock_path = lock_file_path();
    fs::create_dir_all(lock_path.parent().expect("lock path parent"))
        .expect("create env lock directory");
    let deadline = Instant::now() + ENV_LOCK_TIMEOUT;

    loop {
        match OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&lock_path)
        {
            Ok(mut file) => {
                let _ = writeln!(file, "pid={}", std::process::id());
                return EnvLockGuard { lock_path };
            }
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
                if Instant::now() >= deadline {
                    panic!(
                        "timed out waiting for env lock at {}; remove stale file if needed",
                        lock_path.display()
                    );
                }
                thread::sleep(ENV_LOCK_RETRY_INTERVAL);
            }
            Err(error) => {
                panic!(
                    "failed to acquire env lock at {}: {error}",
                    lock_path.display()
                );
            }
        }
    }
}

impl Drop for EnvLockGuard {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.lock_path);
    }
}

fn lock_file_path() -> PathBuf {
    let mut hasher = DefaultHasher::new();
    env!("CARGO_MANIFEST_DIR").hash(&mut hasher);
    let suffix = hasher.finish();
    std::env::temp_dir()
        .join("delta_bench_test_locks")
        .join(format!("env_lock_{suffix:016x}.lock"))
}
