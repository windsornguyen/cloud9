fn main() -> Result<(), Box<dyn std::error::Error>> {
    connectrpc_build::Config::new()
        .files(&["proto/cloud9/kv/v1/kv.proto"])
        .includes(&["proto"])
        .include_file("_cloud9_connect.rs")
        .compile()?;
    Ok(())
}
