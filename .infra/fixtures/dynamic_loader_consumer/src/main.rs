use std::error::Error;
use std::path::PathBuf;

use qubit_magika::MagikaMimeDetector;

fn main() -> Result<(), Box<dyn Error>> {
    let runtime = PathBuf::from(std::env::var("ORT_LIBRARY_PATH")?);
    if !ort::init_from(runtime)?.commit() {
        return Err("ONNX Runtime environment was already initialized".into());
    }
    let _detector = MagikaMimeDetector::new()?;
    Ok(())
}
