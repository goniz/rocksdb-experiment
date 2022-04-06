use super::FakeStorageImpl;
use anyhow::{Context, Result};
use async_trait::async_trait;
use sled::Db;

#[derive(Clone)]
pub struct Sled {
    db: Db,
}

impl Sled {
    pub fn new(path: &str) -> Result<Self> {
        let config = sled::Config::new()
            .path(path)
            .mode(sled::Mode::LowSpace)
            .cache_capacity(1024 * 1024 * 1024)
            .use_compression(false)
            .print_profile_on_drop(true);

        Ok(Self {
            db: config.open().context("Failed to create DB")?,
        })
    }
}

#[async_trait]
impl FakeStorageImpl for Sled {
    fn add_camera(&self, name: &str) {
        let _ = self.db.open_tree(name);
    }

    async fn write_image(&self, camera: &str, index: u64, image: &[u8]) -> Result<()> {
        let db = self.db.clone();
        let camera = camera.to_owned();
        let key = format!("{}.jpg", index);
        let image = image.to_owned();

        tokio::task::spawn_blocking(move || -> Result<()> {
            db.open_tree(&camera)
                .context("open_tree() failed")
                .and_then(|tree| tree.insert(&key, image).context("tree.insert() failed"))
                .map(|_| ())
        })
        .await?
    }

    async fn read_image(&self, camera: &str, index: u64) -> Result<Option<Vec<u8>>> {
        let db = self.db.clone();
        let camera = camera.to_owned();
        let key = format!("{}.jpg", index);

        tokio::task::spawn_blocking(move || -> Result<Option<Vec<u8>>> {
            db.open_tree(&camera)
                .context("open_tree() failed")
                .and_then(|tree| {
                    tree.get(&key)
                        .context("tree.get() failed")
                        .map(|opt| opt.map(|ivec| ivec.to_vec()))
                })
        })
        .await?
    }
}
