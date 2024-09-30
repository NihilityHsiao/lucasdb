use std::{fs, io, path::PathBuf};

pub fn create_dir_if_not_exist(path: &PathBuf) -> Result<(), std::io::Error> {
    if !path.is_dir() {
        fs::create_dir_all(path)?
    }

    Ok(())
}
