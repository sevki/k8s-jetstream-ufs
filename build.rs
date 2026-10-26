fn main() {
    let protoc = protoc_bin_vendored::protoc_bin_path().expect("vendored protoc");
    std::env::set_var("PROTOC", protoc);
    tonic_prost_build::configure()
        .build_client(false)
        .compile_protos(&["proto/csi.proto"], &["proto"])
        .expect("compile csi.proto");
    println!("cargo:rerun-if-changed=proto/csi.proto");
}
