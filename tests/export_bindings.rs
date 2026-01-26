#[path = "../src/types/mod.rs"]
mod types;

#[test]
fn export_bindings() {
    let out_path = "../modern-product-repo/packages/api/src/generated/bindings.ts";
    let ts_cfg =
        specta::ts::ExportConfiguration::default().bigint(specta::ts::BigIntExportBehavior::Number);

    specta::export::ts_with_cfg(out_path, &ts_cfg)
        .expect("failed to export Specta bindings");
}
