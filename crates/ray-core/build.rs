//! Build script for `ray-core`.
//!
//! Compiles `common.proto` to generate the shared protobuf types.
//! These types are used by `proto_conv.rs` for conversion between
//! proto messages and core Rust types.

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let proto_dir = "../../proto";
    let proto_files = &["../../proto/common.proto"];

    let file_descriptor_set = protox::compile(proto_files, &[proto_dir])?;

    tonic_build::configure()
        .build_server(false)
        .build_client(false)
        .out_dir("src/")
        .compile_fds(file_descriptor_set)?;

    for proto_file in proto_files {
        println!("cargo:rerun-if-changed={}", proto_file);
    }

    Ok(())
}
