//! `asap-collector --print-capabilities` prints the golden capabilities and
//! exits 0.
#![cfg(feature = "collector")]

#[test]
fn print_capabilities_prints_the_golden_json() {
    let output = std::process::Command::new(env!("CARGO_BIN_EXE_asap-collector"))
        .arg("--print-capabilities")
        .output()
        .expect("run asap-collector");
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).unwrap(),
        include_str!("golden/capabilities.json")
    );
}
