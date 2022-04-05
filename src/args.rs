#[derive(clap::Parser)]
pub struct Arguments {
    #[clap(long, default_value = "./db")]
    pub db_path: String,

    #[clap(long, arg_enum, required(true))]
    pub db_type: DatabaseType,

    #[clap(long)]
    pub seed_db: bool,

    #[clap(long, default_value = "150000")]
    pub seed_images_per_camera: u64,

    #[clap(long, default_value = "8")]
    pub ram_buffer_mb: usize,

    #[clap(long, default_value = "2")]
    pub n_cameras: usize,

    #[clap(long, default_value = "100")]
    pub n_readers: usize,

    #[clap(long, default_value = "10")]
    pub readers_rate: usize,
}

#[derive(clap::ArgEnum, Clone, Debug)]
pub enum DatabaseType {
    Filesystem,
    RocksDB,
}
