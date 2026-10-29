// Generates Rust types + tonic service stubs from the .proto files.
//   status.proto  — status.StatusService/CheckStatus (analytics event batches)
//   dm_sdk.proto  — dm.sdk.v1 Crashlytics / RemoteConfig / Performance / Messaging services
// google.protobuf.Struct is mapped to prost_types::Struct automatically.
// dm.sdk.v1 also gets proto3-JSON serde impls (pbjson), so every RPC is callable as plain JSON at
// POST /v1/rpc/<service>/<method> — what the Android and JS SDKs use.
use std::path::PathBuf;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let out = PathBuf::from(std::env::var("OUT_DIR")?);
    let descriptors = out.join("descriptors.bin");
    tonic_build::configure()
        .file_descriptor_set_path(&descriptors)
        .compile_protos(&["status.proto", "dm_sdk.proto"], &["."])?;
    pbjson_build::Builder::new()
        .register_descriptors(&std::fs::read(&descriptors)?)?
        .build(&[".dm.sdk.v1"])?;
    println!("cargo:rerun-if-changed=status.proto");
    println!("cargo:rerun-if-changed=dm_sdk.proto");
    Ok(())
}
