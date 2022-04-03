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
        let key = format!("{}.jpg", index);

        let cf = self
            .db
            .cf_handle(camera)
            .context("Failed to open cf handle")?;

        // TODO: maybe use tokio::spawn_blocking()
        self.db
            .put_cf(&cf, key, image)
            .context("Failed to put_cf()")?;

        Ok(())
    }

    async fn read_image(&self, camera: &str, index: u64) -> Result<Option<Vec<u8>>> {
        let key = format!("{}.jpg", index);

        let cf = self
            .db
            .cf_handle(camera)
            .context("Failed to open cf handle")?;

        self.db.get_cf(&cf, &key).context("Key not found")
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
