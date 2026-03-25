use std::collections::BTreeMap;
use std::fs;
use std::path::PathBuf;

use delta_bench::cli::validate_label;
use serde::Deserialize;

#[derive(Debug, Deserialize)]
struct LabelContract {
    valid: Vec<String>,
    invalid: Vec<String>,
    sanitized: BTreeMap<String, String>,
}

fn load_contract() -> LabelContract {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../python/tests/fixtures/label_contract.json");
    serde_json::from_str(&fs::read_to_string(path).expect("label contract fixture"))
        .expect("valid label contract JSON")
}

#[test]
fn cli_validation_accepts_safe_label_chars() {
    let contract = load_contract();
    for label in contract.valid {
        validate_label(&label).expect("label should be valid");
    }
}

#[test]
fn cli_validation_rejects_contract_invalid_labels() {
    let contract = load_contract();
    assert!(
        contract.sanitized.contains_key("feature/foo"),
        "expected sanitized fixtures to be present"
    );
    for label in contract.invalid {
        let err = validate_label(&label).expect_err("label should be rejected");
        assert!(
            err.to_string().contains("label"),
            "unexpected error for '{label}': {err}"
        );
    }
}
