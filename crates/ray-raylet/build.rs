//! Build script for `ray-raylet`.
//!
//! Compiles `raylet.proto` and `common.proto` using protox (no protoc needed).

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let proto_dir = "../../proto";
    let proto_files = &["../../proto/raylet.proto", "../../proto/common.proto"];

    let file_descriptor_set = protox::compile(proto_files, &[proto_dir])?;

    tonic_build::configure()
        .build_server(true)
        .build_client(true)
        .out_dir("src/")
        .compile_fds(file_descriptor_set)?;

    for proto_file in proto_files {
        println!("cargo:rerun-if-changed={}", proto_file);
    }

    Ok(())
}
