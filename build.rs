use std::path::PathBuf;

#[path = "src/types/mod.rs"]
mod types;

fn main() {
    let out_dir = PathBuf::from(
        "/Users/erayack/Desktop/code/modern-product-repo/packages/api/src/generated",
    );

    if !out_dir.is_dir() {
        println!(
            "cargo:warning=Specta bindings output dir not found; skipping generation: {}",
            out_dir.display()
        );
        return;
    }

    let out_file = out_dir.join("bindings.ts");

    let out_file_str = out_file.to_str().expect("bindings path is valid utf-8");
    let ts_cfg = specta::ts::ExportConfiguration::default()
        .bigint(specta::ts::BigIntExportBehavior::Number);
    if let Err(err) = specta::export::ts_with_cfg(out_file_str, &ts_cfg) {
        if let specta::ts::TsExportError::Io(io_err) = &err {
            if io_err.kind() == std::io::ErrorKind::PermissionDenied {
                println!(
                    "cargo:warning=Specta bindings output not writable; skipping generation: {}",
                    out_file.display()
                );
                return;
            }
        }
        panic!("failed to export Specta bindings to {}: {err}", out_file.display());
    }

    println!("cargo:rerun-if-changed=build.rs");
    println!("cargo:rerun-if-changed=src/types/mod.rs");
    println!("cargo:rerun-if-changed=src/types/webhook_event.rs");
    println!("cargo:rerun-if-changed=src/types/webhook_attempt_log.rs");
    println!("cargo:rerun-if-changed=src/types/target_circuit_state.rs");
    println!("cargo:rerun-if-changed=src/types/dispatcher.rs");
}
