#[cfg(unix)]
mod tests {
    use std::fs;
    use std::os::unix::fs::PermissionsExt;

    use delta_bench::system::probe_python_modules;

    fn make_executable(path: &std::path::Path) {
        let mut perms = fs::metadata(path).expect("metadata").permissions();
        perms.set_mode(0o755);
        fs::set_permissions(path, perms).expect("chmod");
    }

    #[test]
    fn probe_python_modules_reports_missing_dependencies() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let fake_python = tmp.path().join("fake-python");

        fs::write(
            &fake_python,
            "#!/usr/bin/env sh\necho '{\"pandas\": true, \"polars\": false, \"pyarrow\": true}'\n",
        )
        .expect("write fake executable");
        make_executable(&fake_python);

        let result = probe_python_modules(
            fake_python.to_string_lossy().as_ref(),
            &["pandas", "polars", "pyarrow"],
        );

        assert_eq!(result.missing_modules, vec!["polars".to_string()]);
        assert!(result.probe_error.is_none(), "unexpected error: {result:?}");
    }

    #[test]
    fn probe_python_modules_reports_probe_errors() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let fake_python = tmp.path().join("fake-python-fails");

        fs::write(
            &fake_python,
            "#!/usr/bin/env sh\necho 'boom' 1>&2\nexit 7\n",
        )
        .expect("write fake executable");
        make_executable(&fake_python);

        let result = probe_python_modules(
            fake_python.to_string_lossy().as_ref(),
            &["pandas", "polars", "pyarrow"],
        );

        assert!(result.probe_error.is_some(), "expected probe error");
        assert!(result.missing_modules.is_empty());
    }
}
