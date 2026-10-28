// Generates Rust types + tonic service stubs from status.proto.
// google.protobuf.Struct is mapped to prost_types::Struct automatically.
fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_build::compile_protos("status.proto")?;
    Ok(())
}
