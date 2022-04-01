use std::sync::Arc;

use anyhow::{Context, Result};
use rocksdb::{BoundColumnFamily, DB};

#[tokio::main]
async fn main() {
    let db_path = "/data/dev/rocksdb/db";
    let db = create_with_existing_cf(db_path).expect("create_with_cf");

    let camera1_cf = try_create_cf(&db, "camera_1").expect("camera1_cf");
    let camera2_cf = try_create_cf(&db, "camera_2").expect("camera2_cf");

    write_camera_images(Arc::clone(&camera1_cf), 100)
        .await
        .expect("Write Camera 1 Images");
}

async fn write_camera_images(camera_cf: Arc<BoundColumnFamily<'_>>, n_images: usize) -> Result<()> {
    // tokio::time::interval_at()
    todo!();
}

fn create_with_existing_cf(db_path: &str) -> Result<DB, rocksdb::Error> {
    let mut opts = rocksdb::Options::default();
    opts.create_if_missing(true);

    match DB::list_cf(&opts, db_path) {
        Ok(cfs) => {
            println!("CF: {:?}", &cfs);
            DB::open_cf(&opts, db_path, &cfs)
        }
        Err(_) => DB::open(&opts, db_path),
    }
}

fn try_create_cf<'a>(db: &'a DB, name: &str) -> Result<Arc<BoundColumnFamily<'a>>> {
    let opts = rocksdb::Options::default();
    let _ = db.create_cf(name, &opts);

    db.cf_handle(name)
        .ok_or_else(|| anyhow::anyhow!("Failed Fetching CF"))
}
