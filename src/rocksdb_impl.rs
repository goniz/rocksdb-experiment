use std::sync::Arc;

use super::FakeStorageImpl;
use anyhow::{Context, Result};
use async_trait::async_trait;
use rocksdb::DB;

#[derive(Clone)]
pub struct RocksDB {
    db: Arc<DB>,
}

impl RocksDB {
    pub fn new(path: &str) -> Result<Self> {
        Ok(Self {
            db: create_with_existing_cf(path).context("Failed to create DB")?,
        })
    }
}

#[async_trait]
impl FakeStorageImpl for RocksDB {
    fn add_camera(&self, name: &str) {
        let opts = rocksdb::Options::default();
        let _ = self.db.create_cf(name, &opts);
    }

    async fn write_image(&self, camera: &str, index: u64, image: &[u8]) -> Result<()> {
        let db = self.db.clone();
        let camera = camera.to_owned();
        let key = format!("{}.jpg", index);
        let image = image.to_owned();

        tokio::task::spawn_blocking(move || -> Result<()> {
            let cf = db.cf_handle(&camera).context("Failed to open cf handle")?;
            db.put_cf(&cf, key, &image).context("Failed to put_cf()")?;

            Ok(())
        })
        .await?
    }

    async fn read_image(&self, camera: &str, index: u64) -> Result<Option<Vec<u8>>> {
        let db = self.db.clone();
        let camera = camera.to_owned();
        let key = format!("{}.jpg", index);

        tokio::task::spawn_blocking(move || -> Result<Option<Vec<u8>>> {
            let cf = db.cf_handle(&camera).context("Failed to open cf handle")?;
            db.get_cf(&cf, &key).context("Key not found")
        })
        .await?
    }
}

fn create_with_existing_cf(db_path: &str) -> Result<Arc<DB>, rocksdb::Error> {
    let mut opts = rocksdb::Options::default();
    opts.create_if_missing(true);

    match DB::list_cf(&opts, db_path) {
        Ok(cfs) => {
            println!("CF: {:?}", &cfs);
            DB::open_cf(&opts, db_path, &cfs)
        }
        Err(_) => DB::open(&opts, db_path),
    }
    .map(Arc::new)
}
