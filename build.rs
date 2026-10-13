fn main() {
    tonic_prost_build::configure()
        .file_descriptor_set_path(format!(
            "{}/catalog_order_descriptor.bin",
            std::env::var("OUT_DIR").expect("OUT_DIR not set")
        ))
        .compile_protos(&["proto/catalog.proto", "proto/orders.proto"], &["proto"])
        .expect("Failed to compile protobuf");
}
