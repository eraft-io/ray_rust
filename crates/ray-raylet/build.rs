//! Build script for `ray-raylet`.
//!
//! Only compiles `raylet.proto`; `common.proto` is resolved for imports
//! but not compiled — its types come from `ray_core::proto::common`.

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let proto_dir = "../../proto";
    let proto_files = &["../../proto/raylet.proto"];

    let file_descriptor_set = protox::compile(proto_files, &[proto_dir])?;

    tonic_build::configure()
        .build_server(true)
        .build_client(true)
        .extern_path(".ray.common", "ray_core::proto::common")
        .out_dir("src/")
        .compile_fds(file_descriptor_set)?;

    println!("cargo:rerun-if-changed=../../proto/raylet.proto");
    println!("cargo:rerun-if-changed=../../proto/common.proto");

    Ok(())
}
