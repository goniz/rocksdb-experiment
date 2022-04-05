use std::time::Duration;

use anyhow::Result;
use async_trait::async_trait;
use clap::Parser;
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

mod args;

#[derive(Clone)]
pub enum SupportedDatabase {
    Filesystem(filesystem_impl::FilesystemStorage),
    RocksDB(rocksdb_impl::RocksDB),
}

#[tokio::main]
async fn main() {
    let args = args::Arguments::parse();

    let db: SupportedDatabase = match args.db_type {
        args::DatabaseType::Filesystem => SupportedDatabase::Filesystem(
            filesystem_impl::FilesystemStorage::new(&args.db_path).expect("FilesystemStorage::new"),
        ),
        args::DatabaseType::RocksDB => SupportedDatabase::RocksDB(
            rocksdb_impl::RocksDB::new(&args.db_path).expect("RocksDB::new"),
        ),
    };

    let cameras: Vec<String> = (0..args.n_cameras)
        .map(|idx| format!("camera{}", idx))
        .collect();

    for camera in &cameras {
        db.add_camera(camera);
    }

    let writers_start_index: u64 = if args.seed_db {
        seed_db(&db, &cameras, args.seed_images_per_camera)
            .await
            .expect("seed_db")
    } else {
        args.seed_images_per_camera
    };

    let (ptr, layout) = unsafe {
        let layout =
            std::alloc::Layout::from_size_align(args.ram_buffer_mb * 1024 * 1024, 8).unwrap();
        let ptr = std::alloc::alloc(layout);

        std::ptr::write_bytes(ptr, 1_u8, layout.size());

        (ptr, layout)
    };

    let writers: Vec<JoinHandle<()>> = spawn_writers(db.clone(), &cameras, writers_start_index);
    let readers: Vec<JoinHandle<()>> = spawn_readers(
        db.clone(),
        &cameras,
        0,
        writers_start_index,
        args.n_readers,
        args.readers_rate,
    );

    let _ = futures::future::join_all(writers.into_iter().chain(readers.into_iter())).await;

    unsafe {
        std::alloc::dealloc(ptr, layout);
    }
}

fn spawn_readers(
    db: impl FakeStorageImpl,
    cameras: &[String],
    start_index: u64,
    end_index: u64,
    n_readers: usize,
    rate: usize,
) -> Vec<JoinHandle<()>> {
    (0..n_readers)
        .map(|_| {
            let camera = cameras
                .iter()
                .choose(&mut rand::thread_rng())
                .cloned()
                .expect("choose");

            let db_cloned = db.clone();
            tokio::task::spawn(async move {
                read_images(db_cloned, &camera, start_index, end_index, rate)
                    .await
                    .expect("Read Camera 2 Images");
            })
        })
        .collect()
}

fn spawn_writers(
    db: impl FakeStorageImpl,
    cameras: &[String],
    start_index: u64,
) -> Vec<JoinHandle<()>> {
    cameras
        .iter()
        .map(|camera| {
            let db_cloned = db.clone();
            let camera_name = camera.clone();
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
    cameras: &[String],
    images_per_camera: u64,
) -> Result<u64> {
    println!("Seeding DB before benchmark");

    let jpeg_buffer: Vec<u8> = std::iter::repeat_with(rand::random::<u8>)
        .take(140 * 1024)
        .collect();

    for index in 0..images_per_camera {
        for camera_id in cameras {
            db.write_image(camera_id, index, &jpeg_buffer).await?;
        }
    }

    Ok(images_per_camera)
}

async fn read_images(
    db: impl FakeStorageImpl,
    camera_name: &str,
    start_index: u64,
    end_index: u64,
    rate: usize,
) -> Result<()> {
    let interval = tokio::time::interval_at(Instant::now(), Duration::from_secs(1) / rate as u32);
    let stream = IntervalStream::new(interval);

    pin_mut!(stream);

    let mut max_duration = Duration::from_secs(0);

    println!("[{}] Starting read task", camera_name);

    while (stream.next().await).is_some() {
        let index = (start_index..end_index)
            .choose(&mut rand::thread_rng())
            .unwrap();

        let start = Instant::now();
        let _ = db.read_image(camera_name, index).await?;

        let elapsed = start.elapsed();
        if elapsed > max_duration {
            max_duration = elapsed;
            println!("[{}] Max read time: {:?}", camera_name, elapsed);
        }
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

#[async_trait]
impl FakeStorageImpl for SupportedDatabase {
    fn add_camera(&self, name: &str) {
        match self {
            SupportedDatabase::Filesystem(fs) => fs.add_camera(name),
            SupportedDatabase::RocksDB(rocks) => rocks.add_camera(name),
        }
    }

    async fn write_image(&self, camera: &str, index: u64, image: &[u8]) -> Result<()> {
        match self {
            SupportedDatabase::Filesystem(fs) => fs.write_image(camera, index, image).await,
            SupportedDatabase::RocksDB(rocks) => rocks.write_image(camera, index, image).await,
        }
    }

    async fn read_image(&self, camera: &str, index: u64) -> Result<Option<Vec<u8>>> {
        match self {
            SupportedDatabase::Filesystem(fs) => fs.read_image(camera, index).await,
            SupportedDatabase::RocksDB(rocks) => rocks.read_image(camera, index).await,
        }
    }
}
