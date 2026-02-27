use delta_bench::suites::merge_dml::{merge_case_by_name, merge_case_names};

#[test]
fn known_merge_case_names_are_present() {
    let names = merge_case_names();
    assert!(names
        .iter()
        .any(|name| name.contains("upsert_filesMatchedFraction_0.05_rowsMatchedFraction_0.1")));
    assert!(names.iter().any(
        |name| name.contains("delete_only_filesMatchedFraction_0.05_rowsMatchedFraction_0.05")
    ));
}

#[test]
fn merge_case_lookup_is_case_insensitive() {
    let c = merge_case_by_name("DELETE_ONLY_FILESMATCHEDFRACTION_0.05_ROWSMATCHEDFRACTION_0.05");
    assert!(c.is_some());
}
