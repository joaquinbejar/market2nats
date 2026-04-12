fn main() -> Result<(), Box<dyn std::error::Error>> {
    prost_build::compile_protos(&["proto/market_data.proto"], &["proto/"])?;
    Ok(())
}
