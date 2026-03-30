pub struct EnvVarRestoreGuard {
    previous: Vec<(String, Option<std::ffi::OsString>)>,
}

impl EnvVarRestoreGuard {
    pub fn set(entries: &[(&str, &str)]) -> Self {
        let previous = entries
            .iter()
            .map(|(key, _)| ((*key).to_string(), std::env::var_os(key)))
            .collect::<Vec<_>>();
        for (key, value) in entries {
            // Safety: call sites are expected to hold the integration-test env lock so
            // no concurrent threads mutate process environment while overrides are active.
            unsafe { std::env::set_var(key, value) };
        }
        Self { previous }
    }
}

impl Drop for EnvVarRestoreGuard {
    fn drop(&mut self) {
        for (key, value) in self.previous.drain(..) {
            if let Some(value) = value {
                // Safety: the same env lock used during setup remains held until the
                // restore guard is dropped, so restoration is also serialized.
                unsafe { std::env::set_var(&key, value) };
            } else {
                // Safety: restoration runs under the same serialized test env contract.
                unsafe { std::env::remove_var(&key) };
            }
        }
    }
}

/// Run an async closure with temporary environment variable overrides.
/// Variables are restored when the closure completes (via RAII guard).
pub async fn with_env_vars<F, Fut, T>(entries: &[(&str, &str)], f: F) -> T
where
    F: FnOnce() -> Fut,
    Fut: std::future::Future<Output = T>,
{
    let _restore_guard = EnvVarRestoreGuard::set(entries);
    f().await
}
