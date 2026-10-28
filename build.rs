// Generates Rust types + tonic service stubs from the .proto files.
//   status.proto  — status.StatusService/CheckStatus (analytics event batches)
//   dm_sdk.proto  — dm.sdk.v1 Crashlytics / RemoteConfig / Performance / Messaging services
// google.protobuf.Struct is mapped to prost_types::Struct automatically.
fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_build::configure().compile_protos(&["status.proto", "dm_sdk.proto"], &["."])?;
    println!("cargo:rerun-if-changed=status.proto");
    println!("cargo:rerun-if-changed=dm_sdk.proto");
    Ok(())
}
