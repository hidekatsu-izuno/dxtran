//! Command-line parsing and resource defaults.
use crate::config;
use clap::{Arg, Args, CommandFactory, FromArgMatches, Parser, Subcommand};
use std::{collections::BTreeSet, ffi::OsString, path::PathBuf};

#[derive(Parser)]
#[command(
    name = "dxt",
    version,
    about = "SQL batch conversion for CSV, fixed, RDW and Parquet files"
)]
pub struct Cli {
    #[command(subcommand)]
    pub(super) command: Command,
    #[arg(skip)]
    pub(super) paths: config::PathOverrides,
}
const PATH_HELP: &str = "File paths (override YAML; relative to the current directory):\n  --input.<id>.path <PATH>\n  --output.<id>.path <PATH>\nExample: --input.sales.path=sales.csv";
#[derive(Subcommand)]
pub(super) enum Command {
    #[command(after_help = PATH_HELP)]
    Check {
        job: PathBuf,
        #[arg(long)]
        scan: bool,
        #[command(flatten)]
        resources: Resources,
    },
    #[command(after_help = PATH_HELP)]
    Explain {
        job: PathBuf,
        #[command(flatten)]
        resources: Resources,
    },
    #[command(after_help = PATH_HELP)]
    Run {
        job: PathBuf,
        #[command(flatten)]
        resources: Resources,
    },
}
pub(super) fn parse_cli(args: impl IntoIterator<Item = OsString>) -> clap::error::Result<Cli> {
    let args: Vec<_> = args.into_iter().collect();
    // Register dataset-specific options before letting clap parse values and
    // report unknown options, duplicate arguments, and missing values.
    let mut flags = BTreeSet::new();
    for arg in args.iter().skip(1).take_while(|arg| *arg != "--") {
        let arg = arg.to_string_lossy();
        let flag = arg.split('=').next().unwrap();
        if let Some(flag) = flag.strip_prefix("--")
            && let Some((section, rest)) = flag.split_once('.')
            && matches!(section, "input" | "output")
            && let Some(id) = rest.strip_suffix(".path")
            && config::identifier(id)
        {
            flags.insert(flag.to_owned());
        }
    }
    let command = Cli::command().mut_subcommands(|subcommand| {
        subcommand.args(flags.iter().map(|flag| {
            Arg::new(flag.clone())
                .long(flag.clone())
                .value_name("PATH")
                .value_parser(clap::value_parser!(PathBuf))
        }))
    });
    let matches = command.try_get_matches_from(args)?;
    let mut paths = config::PathOverrides::default();
    if let Some((_, subcommand)) = matches.subcommand() {
        for flag in flags {
            if let Some(path) = subcommand.get_one::<PathBuf>(&flag).cloned() {
                let (section, rest) = flag.split_once('.').unwrap();
                let id = rest.strip_suffix(".path").unwrap().to_owned();
                if section == "input" {
                    paths.input.insert(id, path);
                } else {
                    paths.output.insert(id, path);
                }
            }
        }
    }
    let mut cli = Cli::from_arg_matches(&matches)?;
    cli.paths = paths;
    Ok(cli)
}
#[derive(Clone, Debug, Args)]
pub struct Resources {
    #[arg(long,default_value="512MiB",value_parser=parse_memory)]
    pub memory_limit: usize,
    #[arg(long)]
    pub temp_dir: Option<PathBuf>,
    #[arg(long,default_value_t=default_threads(),value_parser=positive)]
    pub threads: usize,
}
pub fn default_threads() -> usize {
    default_threads_for(
        std::thread::available_parallelism()
            .map(usize::from)
            .unwrap_or(1),
    )
}
pub(super) fn default_threads_for(cpus: usize) -> usize {
    (cpus / 2).clamp(1, 4)
}
pub(super) fn positive(s: &str) -> std::result::Result<usize, String> {
    s.parse::<usize>()
        .ok()
        .filter(|n| *n > 0)
        .ok_or_else(|| "must be a positive integer".into())
}
pub fn parse_memory(s: &str) -> std::result::Result<usize, String> {
    let split = s.find(|c: char| !c.is_ascii_digit()).unwrap_or(s.len());
    let n = s[..split]
        .parse::<usize>()
        .map_err(|_| "invalid memory limit")?;
    let multiplier = match &s[split..] {
        "" | "B" => 1,
        "KiB" => 1024,
        "MiB" => 1024 * 1024,
        "GiB" => 1024 * 1024 * 1024,
        "KB" => 1000,
        "MB" => 1_000_000,
        "GB" => 1_000_000_000,
        _ => return Err("use B, KiB, MiB or GiB".into()),
    };
    n.checked_mul(multiplier)
        .filter(|n| *n > 0)
        .ok_or_else(|| "memory limit must be positive and fit usize".into())
}
