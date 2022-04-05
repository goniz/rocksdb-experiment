use std::time::Duration;

use anyhow::Result;
use async_trait::async_trait;
use futures::pin_mut;
use rand::prelude::IteratorRandom;
use tokio::{task::JoinHandle, time::Instant};
use tokio_stream::{wrappers::IntervalStream, StreamExt};

#[async_trait]
pub trait FakeStorageImpl: Clone + Send + Sync + 'static {
    fn add_camera(&self, name: &str);

    async fn write_image(&self, camera: &str, index: u64, image: &[u8]) -> Result<()>;

    async fn read_image(&self, camera: &str, index: u64) -> Result<Option<Vec<u8>>>;
}

#[allow(dead_code)]
mod filesystem_impl;

#[allow(dead_code)]
mod rocksdb_impl;

#[tokio::main]
async fn main() {
    let db_path = "db";
    let cameras = vec!["camera1", "camera2"];
    let n_readers: usize = 100;
    let readers_rate: usize = 10;
    let seed = true;
    let seed_images_per_camera = 150_000;

    let db = rocksdb_impl::RocksDB::new(db_path).expect("RocksDB::new");
    // let db = filesystem_impl::FilesystemStorage::new(db_path).expect("FilesystemStorage::new");

    for camera in &cameras {
        db.add_camera(camera);
    }

    let start_index: u64 = if seed {
        seed_db(&db, &cameras, seed_images_per_camera)
            .await
            .expect("seed_db")
    } else {
        seed_images_per_camera
    };

    let writers: Vec<JoinHandle<()>> = spawn_writers(db.clone(), &cameras, start_index);
    let readers: Vec<JoinHandle<()>> =
        spawn_readers(db.clone(), &cameras, 0, n_readers, readers_rate);

    let _ = futures::future::join_all(writers.into_iter().chain(readers.into_iter())).await;
}

fn spawn_readers(
    db: impl FakeStorageImpl,
    cameras: &[&str],
    start_index: u64,
    n_readers: usize,
    rate: usize,
) -> Vec<JoinHandle<()>> {
    (0..n_readers)
        .map(|_| {
            let camera = cameras
                .iter()
                .choose(&mut rand::thread_rng())
                .cloned()
                .expect("choose")
                .to_owned();

            let db_cloned = db.clone();
            tokio::task::spawn(async move {
                read_images(
                    db_cloned,
                    &camera,
                    start_index,
                    Duration::from_secs(0),
                    rate,
                )
                .await
                .expect("Read Camera 2 Images");
            })
        })
        .collect()
}

fn spawn_writers(
    db: impl FakeStorageImpl,
    cameras: &[&str],
    start_index: u64,
) -> Vec<JoinHandle<()>> {
    cameras
        .iter()
        .map(|&camera| {
            let db_cloned = db.clone();
            let camera_name = camera.to_owned();
            tokio::task::spawn(async move {
                write_camera_images(db_cloned, &camera_name, start_index)
                    .await
                    .expect("Write Camera 1 Images");
            })
        })
        .collect()
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

async fn read_images(
    db: impl FakeStorageImpl,
    camera_name: &str,
    start_index: u64,
    initial_delay: Duration,
    rate: usize,
) -> Result<()> {
    println!(
        "[{}] Read Task: Waiting for {} seconds",
        camera_name,
        initial_delay.as_secs()
    );

    tokio::time::sleep(initial_delay).await;

    let interval = tokio::time::interval_at(Instant::now(), Duration::from_secs(1) / rate as u32);
    let stream = IntervalStream::new(interval);

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
