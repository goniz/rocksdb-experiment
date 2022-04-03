use std::{sync::Arc, time::Duration};

use anyhow::{Context, Result};
use futures::pin_mut;
use rocksdb::DB;
use tokio::{join, time::Instant};
use tokio_stream::{wrappers::IntervalStream, StreamExt};

#[tokio::main]
async fn main() {
    let db_path = "db";
    let db = create_with_existing_cf(db_path).expect("create_with_cf");

    try_create_cf(&db, "camera_1");
    try_create_cf(&db, "camera_2");

    let start_index: u64 = 10_000;

    let camera1_write_db = db.clone();
    let camera1_write_handle = tokio::task::spawn(async move {
        write_camera_images(camera1_write_db, "camera_1", start_index)
            .await
            .expect("Write Camera 1 Images");
    });

    let camera2_write_db = db.clone();
    let camera2_write_handle = tokio::task::spawn(async move {
        write_camera_images(camera2_write_db, "camera_2", start_index)
            .await
            .expect("Write Camera 2 Images");
    });

    let camera2_read_db = db.clone();
    let camera2_read_handle = tokio::task::spawn(async move {
        read_camera_images(
            camera2_read_db,
            "camera_2",
            start_index,
            Duration::from_secs(5),
            1000,
            20,
        )
        .await
        .expect("Read Camera 2 Images");
    });

    let (_, _, _) = join!(
        camera1_write_handle,
        camera2_write_handle,
        camera2_read_handle
    );
}

async fn read_camera_images(
    db: Arc<DB>,
    camera_cf: &str,
    start_index: u64,
    initial_delay: Duration,
    n_images: usize,
    rate: usize,
) -> Result<()> {
    println!(
        "[{}] Read Task: Waiting for {} seconds",
        camera_cf,
        initial_delay.as_secs()
    );

    tokio::time::sleep(initial_delay).await;

    let interval = tokio::time::interval_at(Instant::now(), Duration::from_secs(1) / rate as u32);
    let stream = IntervalStream::new(interval).take(n_images);

    pin_mut!(stream);

    let mut index = start_index;
    let mut max_duration = Duration::from_secs(0);

    println!("[{}] Starting read task", camera_cf);

    while (stream.next().await).is_some() {
        let jpeg_key = format!("{}.jpg", index);

        let start = Instant::now();
        let cf = db
            .cf_handle(camera_cf)
            .context("Failed to open cf handle")?;

        let _value = db.get_cf(&cf, &jpeg_key).context("Failed to get value")?;

        let elapsed = start.elapsed();
        if elapsed > max_duration {
            max_duration = elapsed;
            println!("[{}] Max read time: {:?}", camera_cf, elapsed);
        }

        index += 1;
    }

    Ok(())
}

async fn write_camera_images(db: Arc<DB>, camera_cf: &str, start_index: u64) -> Result<()> {
    let start_time = Instant::now();
    let interval = tokio::time::interval_at(start_time, Duration::from_secs(1) / 20);
    let stream = IntervalStream::new(interval);

    pin_mut!(stream);

    let jpeg_buffer: Vec<u8> = std::iter::repeat_with(rand::random::<u8>)
        .take(140 * 1024)
        .collect();

    let mut index = start_index;
    let mut max_duration = Duration::from_secs(0);

    println!("[{}] Starting write task", camera_cf);

    while (stream.next().await).is_some() {
        let jpeg_key = format!("{}.jpg", index);
        // println!("[CF={}] Writing key '{}'", camera_cf, &jpeg_key);

        let start = Instant::now();
        let cf = db
            .cf_handle(camera_cf)
            .context("Failed to open cf handle")?;

        // TODO: maybe use tokio::spawn_blocking()
        db.put_cf(&cf, jpeg_key, &jpeg_buffer)
            .context("Failed to put_cf()")?;

        let elapsed = start.elapsed();
        if elapsed > max_duration {
            max_duration = elapsed;
            println!("[{}] Max write time: {:?}", camera_cf, elapsed);
        }

        index += 1;
    }

    Ok(())
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

fn try_create_cf(db: &DB, name: &str) {
    let opts = rocksdb::Options::default();
    let _ = db.create_cf(name, &opts);
}
