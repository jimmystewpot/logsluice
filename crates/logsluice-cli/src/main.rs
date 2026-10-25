//! Command line tool for deterministic windowed aggregate replay and offline drain.

#![allow(clippy::print_stdout, clippy::print_stderr)]

mod drain;
mod generate;
mod replay;

use std::path::PathBuf;
use std::process::ExitCode;

use clap::{Args, Parser, Subcommand};

#[derive(Parser)]
#[command(
    name = "logsluice",
    about = "Replay and offline drain tool for logsluice"
)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Deterministically replays synthetic events through ShardWorkers with simulated sink/crashes.
    Replay(ReplayArgs),
    /// Scans live shard stores offline and emits pending deltas as SliceRecords.
    Drain(DrainArgs),
}

/// Command-line arguments for the `logsluice replay` subcommand.
#[derive(Args, Debug)]
pub struct ReplayArgs {
    /// Total number of events to generate.
    #[arg(long, default_value_t = 200_000)]
    pub events: usize,

    /// Number of distinct keys in range [0..keys).
    #[arg(long, default_value_t = 5000)]
    pub keys: usize,

    /// Deterministic PRNG seed.
    #[arg(long, default_value_t = 7)]
    pub seed: u64,

    /// Number of worker shards.
    #[arg(long, default_value_t = 2)]
    pub shards: u16,

    /// Window duration in milliseconds.
    #[arg(long, default_value_t = 10_000)]
    pub window_duration_ms: u64,

    /// Storage root directory.
    #[arg(long)]
    pub data_dir: PathBuf,

    /// Crash workers and store threads every M events without acking in-flight tickets.
    #[arg(long)]
    pub crash_every: Option<usize>,

    /// Simulated negative acknowledgment rate in range [0.0, 1.0].
    #[arg(long)]
    pub nack_rate: Option<f64>,

    /// Flush L1 and exit without closing windows or advancing watermarks past window close.
    #[arg(long)]
    pub stop_before_close: bool,
}

/// Command-line arguments for the `logsluice drain` subcommand.
#[derive(Args, Debug)]
pub struct DrainArgs {
    /// Storage root directory containing shard stores.
    #[arg(long)]
    pub data_dir: PathBuf,

    /// Output serialization format (default: ndjson).
    #[arg(long, default_value = "ndjson")]
    pub format: String,
}

fn main() -> ExitCode {
    let cli = Cli::parse();

    match cli.command {
        Commands::Replay(args) => {
            let opts = replay::ReplayOptions {
                events: args.events,
                keys: args.keys,
                seed: args.seed,
                shards: args.shards,
                window_duration_ms: args.window_duration_ms,
                data_dir: args.data_dir,
                crash_every: args.crash_every,
                nack_rate: args.nack_rate,
                stop_before_close: args.stop_before_close,
                l1_coalesce_max_keys: 100_000,
            };

            match replay::run_replay(opts) {
                Ok(()) => ExitCode::SUCCESS,
                Err(err) => {
                    eprintln!("replay error: {err}");
                    ExitCode::FAILURE
                }
            }
        }
        Commands::Drain(args) => match drain::run_drain(&args.data_dir, &args.format) {
            Ok(()) => ExitCode::SUCCESS,
            Err(err) => {
                eprintln!("drain error: {err}");
                ExitCode::FAILURE
            }
        },
    }
}
