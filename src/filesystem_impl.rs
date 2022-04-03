use std::{path::PathBuf, str::FromStr};

use super::FakeStorageImpl;
use anyhow::{Context, Result};
use async_trait::async_trait;

#[derive(Clone)]
pub struct FilesystemStorage {
    path: PathBuf,
}

impl FilesystemStorage {
    pub fn new(path: &str) -> Result<Self> {
        let path = PathBuf::from_str(path)?;

        std::fs::create_dir_all(&path)?;

        Ok(Self { path })
    }
}

#[async_trait]
impl FakeStorageImpl for FilesystemStorage {
    fn add_camera(&self, name: &str) {
        let camera_path = self.path.join(name);
        std::fs::create_dir_all(&camera_path).expect("mkdir should work");
    }

    async fn write_image(&self, camera: &str, index: u64, image: &[u8]) -> Result<()> {
        let file_name = format!("{}.jpg", index);
        let file_path = self.path.join(camera).join(file_name);

        std::fs::write(file_path, image)?;

        Ok(())
    }

    async fn read_image(&self, camera: &str, index: u64) -> Result<Option<Vec<u8>>> {
        let file_name = format!("{}.jpg", index);
        let file_path = self.path.join(camera).join(file_name);

        // TODO: should return Ok(None) if not found
        Ok(Some(std::fs::read(file_path)?))
    }
}
