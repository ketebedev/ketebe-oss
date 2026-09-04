#[test]
fn manifest_depends_on_public_sdk_not_server_internals() {
    let manifest = include_str!("../Cargo.toml");
    assert!(manifest.contains("ketebe-sdk"));
    for forbidden in ["ketebe-server", "ketebe-storage", "ketebe-core"] {
        assert!(
            !manifest.contains(forbidden),
            "MCP manifest must not depend on {forbidden}"
        );
    }
}
