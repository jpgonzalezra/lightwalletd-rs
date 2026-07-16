use std::path::Path;
use std::process::Command;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let out_dir = std::env::var("OUT_DIR")?;
    let descriptor_path = Path::new(&out_dir).join("file_descriptor_set.bin");

    tonic_prost_build::configure()
        .build_server(true)
        .build_client(true)
        // Emitted so `src/proto.rs` can embed it and register it with `tonic-reflection` at
        // startup (`grpcurl list` / `describe` work against a running server with no local
        // `.proto` files needed).
        .file_descriptor_set_path(&descriptor_path)
        .compile_protos(&["proto/service.proto", "proto/darkside.proto"], &["proto"])?;

    emit_git_commit();
    Ok(())
}

/// Embed the short git commit hash for `GetLightdInfo`. Best-effort: an empty string when git or the
/// repository is unavailable (e.g. building from a source tarball), so the build never fails on it.
fn emit_git_commit() {
    // Refresh on the checked-out commit changing: watch HEAD (checkouts) and the ref it points at
    // (new commits on the branch). Skipped cleanly when there is no working `.git`.
    if let Ok(head) = std::fs::read_to_string(".git/HEAD") {
        println!("cargo:rerun-if-changed=.git/HEAD");
        if let Some(reference) = head.strip_prefix("ref: ").map(str::trim) {
            let ref_path = format!(".git/{reference}");
            if Path::new(&ref_path).exists() {
                println!("cargo:rerun-if-changed={ref_path}");
            }
        }
    }
    let git_commit = Command::new("git")
        .args(["rev-parse", "--short", "HEAD"])
        .output()
        .ok()
        .filter(|out| out.status.success())
        .and_then(|out| String::from_utf8(out.stdout).ok())
        .map(|hash| hash.trim().to_string())
        .unwrap_or_default();
    println!("cargo:rustc-env=GIT_COMMIT={git_commit}");
}
