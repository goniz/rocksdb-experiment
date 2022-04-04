use std::time::Duration;

use anyhow::Result;
use async_trait::async_trait;
use futures::pin_mut;
use tokio::{join, time::Instant};
use tokio_stream::{wrappers::IntervalStream, StreamExt};

#[async_trait]
pub trait FakeStorageImpl {
    fn add_camera(&self, name: &str);

    async fn write_image(&self, camera: &str, index: u64, image: &[u8]) -> Result<()>;

    async fn read_image(&self, camera: &str, index: u64) -> Result<Option<Vec<u8>>>;
}

mod filesystem_impl;
mod rocksdb_impl;

#[tokio::main]
async fn main() {
    let db_path = "db";
    // let db = rocksdb_impl::RocksDB::new(db_path).expect("RocksDB::new");
    let db = filesystem_impl::FilesystemStorage::new(db_path).expect("FilesystemStorage::new");

    db.add_camera("camera1");
    db.add_camera("camera2");

    let start_index: u64 = seed_db(&db, &["camera1", "camera2"], 10_000)
        .await
        .expect("seed_db");

    let camera1_write_db = db.clone();
    let camera1_write_handle = tokio::task::spawn(async move {
        write_camera_images(camera1_write_db, "camera1", start_index)
            .await
            .expect("Write Camera 1 Images");
    });

    let camera2_write_db = db.clone();
    let camera2_write_handle = tokio::task::spawn(async move {
        write_camera_images(camera2_write_db, "camera2", start_index)
            .await
            .expect("Write Camera 2 Images");
    });

    let camera2_read_db = db.clone();
    let camera2_read_handle = tokio::task::spawn(async move {
        read_camera_images(
            camera2_read_db,
            "camera2",
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

async fn seed_db(
    db: &impl FakeStorageImpl,
    cameras: &[&str],
    images_per_camera: u64,
) -> Result<u64> {
    println!("Seeding DB before benchmark");

    let jpeg_buffer: Vec<u8> = std::iter::repeat_with(rand::random::<u8>)
        .take(140 * 1024)
        .collect();

    for index in 0..images_per_camera {
        for &camera_id in cameras {
            db.write_image(camera_id, index, &jpeg_buffer).await?;
        }
    }

    Ok(images_per_camera)
}

async fn read_camera_images(
    db: impl FakeStorageImpl,
    camera_name: &str,
    start_index: u64,
    initial_delay: Duration,
    n_images: usize,
    rate: usize,
) -> Result<()> {
    println!(
        "[{}] Read Task: Waiting for {} seconds",
        camera_name,
        initial_delay.as_secs()
    );

    tokio::time::sleep(initial_delay).await;

    let interval = tokio::time::interval_at(Instant::now(), Duration::from_secs(1) / rate as u32);
    let stream = IntervalStream::new(interval).take(n_images);

    pin_mut!(stream);

    let mut index = start_index;
    let mut max_duration = Duration::from_secs(0);

    println!("[{}] Starting read task", camera_name);

    while (stream.next().await).is_some() {
        let start = Instant::now();
        let _ = db.read_image(camera_name, index).await?;

        let elapsed = start.elapsed();
        if elapsed > max_duration {
            max_duration = elapsed;
            println!("[{}] Max read time: {:?}", camera_name, elapsed);
        }

        index += 1;
    }

    Ok(())
}

async fn write_camera_images(
    db: impl FakeStorageImpl,
    camera_name: &str,
    start_index: u64,
) -> Result<()> {
    let start_time = Instant::now();
    let interval = tokio::time::interval_at(start_time, Duration::from_secs(1) / 20);
    let stream = IntervalStream::new(interval);

    pin_mut!(stream);

    let jpeg_buffer: Vec<u8> = std::iter::repeat_with(rand::random::<u8>)
        .take(140 * 1024)
        .collect();

    let mut index = start_index;
    let mut max_duration = Duration::from_secs(0);

    println!("[{}] Starting write task", camera_name);

    while (stream.next().await).is_some() {
        let start = Instant::now();
        db.write_image(camera_name, index, &jpeg_buffer).await?;

        let elapsed = start.elapsed();
        if elapsed > max_duration {
            max_duration = elapsed;
            println!("[{}] Max write time: {:?}", camera_name, elapsed);
        }

        index += 1;
    }

    Ok(())
}
