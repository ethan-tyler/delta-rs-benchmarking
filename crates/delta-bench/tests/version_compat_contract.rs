use std::fs;
use std::path::{Path, PathBuf};

fn contract_scanned_source_files(root: &Path) -> Vec<PathBuf> {
    all_rust_source_files(root)
}

fn source_files_except_version_compat(root: &Path) -> Vec<PathBuf> {
    contract_scanned_source_files(root)
        .into_iter()
        .filter(|path| {
            path.file_name()
                .map_or(true, |name| name != "version_compat.rs")
        })
        .collect()
}

fn all_rust_source_files(root: &Path) -> Vec<PathBuf> {
    let mut files = Vec::new();
    collect_rust_source_files(&root.join("src"), &mut files);
    files.sort();
    files
}

fn collect_rust_source_files(dir: &Path, files: &mut Vec<PathBuf>) {
    for entry in fs::read_dir(dir).expect("read source dir") {
        let entry = entry.expect("read source entry");
        let path = entry.path();
        if path.is_dir() {
            collect_rust_source_files(&path, files);
        } else if path.extension().is_some_and(|ext| ext == "rs") {
            files.push(path);
        }
    }
}

#[test]
fn version_contract_scans_entire_delta_bench_source_tree() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let all_files = all_rust_source_files(root);
    let covered = contract_scanned_source_files(root);
    assert_eq!(
        covered, all_files,
        "version compatibility contract should cover all delta-bench Rust source files"
    );
}

#[test]
fn delta_rs_version_conversions_use_shared_compat_helpers() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let source_files = contract_scanned_source_files(root);

    for path in source_files {
        let source = fs::read_to_string(&path).expect("read source");
        assert!(
            !source.contains("version().map(|v| v as u64)")
                && !source.contains("version().map(|version| version as u64)"),
            "{path:?} should route delta-rs version conversions through shared compat helpers",
        );
    }
}

#[test]
fn snapshot_version_call_sites_reuse_shared_compat_helpers() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let source_files = source_files_except_version_compat(root);

    for path in source_files {
        let source = fs::read_to_string(&path).expect("read source");
        assert!(
            !source.contains("fn snapshot_version_arg<") && !source.contains("fn version_to_u64<"),
            "{path:?} should reuse shared delta-rs compatibility helpers instead of redefining them locally",
        );
    }
}
