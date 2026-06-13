//! Build script for `ray-gcs`.
//!
//! Compiles the `gcs.proto` and `common.proto` files using `protox`
//! (a pure-Rust protobuf parser that doesn't require `protoc` to be installed).

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let proto_dir = "../../proto";
    let proto_files = &["../../proto/gcs.proto", "../../proto/common.proto"];

    // Use protox as the protobuf parser (no protoc binary required)
    let file_descriptor_set = protox::compile(proto_files, &[proto_dir])?;

    tonic_build::configure()
        .build_server(true)
        .build_client(true)
        .out_dir("src/")
        .compile_fds(file_descriptor_set)?;

    // Re-run if proto files change
    for proto_file in proto_files {
        println!("cargo:rerun-if-changed={}", proto_file);
    }

    Ok(())
}
