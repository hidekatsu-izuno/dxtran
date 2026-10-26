mod cooperative;
mod csv;
mod fixed;
mod signals;
use crate::{
    config,
    error::{Error, Result},
    planner, sink,
    source::InputReader,
};
use clap::{Arg, Args, CommandFactory, FromArgMatches, Parser, Subcommand};
use datafusion::{
    execution::{
        disk_manager::{DiskManagerBuilder, DiskManagerMode},
        memory_pool::FairSpillPool,
        runtime_env::RuntimeEnvBuilder,
        session_state::SessionStateBuilder,
    },
    physical_plan::displayable,
    prelude::{SessionConfig, SessionContext},
};
use futures::StreamExt;
use serde_json::json;
use std::{
    collections::BTreeSet,
    ffi::OsString,
    fs,
    path::{Path, PathBuf},
    sync::Arc,
    time::Instant,
};

#[derive(Parser)]
#[command(
    name = "dxt",
    version,
    about = "SQL batch conversion for CSV, fixed, RDW and Parquet files"
)]
pub struct Cli {
    #[command(subcommand)]
    command: Command,
    #[arg(skip)]
    paths: config::PathOverrides,
}
const PATH_HELP: &str = "File paths (override YAML; relative to the current directory):\n  --input.<id>.path <PATH>\n  --output.<id>.path <PATH>\nExample: --input.sales.path=sales.csv";
#[derive(Subcommand)]
enum Command {
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
fn parse_cli(args: impl IntoIterator<Item = OsString>) -> clap::error::Result<Cli> {
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
fn default_threads_for(cpus: usize) -> usize {
    (cpus / 2).clamp(1, 4)
}
fn positive(s: &str) -> std::result::Result<usize, String> {
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
fn log(value: serde_json::Value) {
    eprintln!("{value}");
}
fn phase(code: u8) -> &'static str {
    match code {
        2 => "configuration",
        3 => "input",
        4 => "sql",
        _ => "output",
    }
}
fn original_error(e: datafusion::error::DataFusionError) -> Error {
    let mut source: Option<&(dyn std::error::Error + 'static)> = Some(&e);
    while let Some(s) = source {
        if let Some(e) = s.downcast_ref::<Error>() {
            return Error::new(e.code, e.message.clone());
        }
        source = s.source();
    }
    use datafusion::error::DataFusionError;
    match e.find_root() {
        DataFusionError::ResourcesExhausted(_)
        | DataFusionError::IoError(_)
        | DataFusionError::Plan(_)
        | DataFusionError::NotImplemented(_)
        | DataFusionError::Configuration(_) => Error::from(e),
        root => {
            let message = root.to_string().to_ascii_lowercase();
            let cause =
                if message.contains("divide by zero") || message.contains("division by zero") {
                    "division by zero"
                } else if message.contains("overflow") {
                    "numeric overflow"
                } else if message.contains("cast") {
                    "type conversion"
                } else {
                    "SQL computation"
                };
            Error::sql(format!("{cause} failed (input value details omitted)"))
        }
    }
}
fn context(resources: &Resources, spill: Option<&Path>) -> Result<SessionContext> {
    let mut runtime = RuntimeEnvBuilder::new()
        .with_memory_pool(Arc::new(FairSpillPool::new(resources.memory_limit)));
    runtime = if let Some(path) = spill {
        runtime.with_temp_file_path(path)
    } else {
        runtime.with_disk_manager_builder(
            DiskManagerBuilder::default().with_mode(DiskManagerMode::Disabled),
        )
    };
    let runtime = runtime.build_arc()?;
    let mut config = SessionConfig::new()
        .with_target_partitions(resources.threads)
        .with_batch_size(8192);
    config.options_mut().execution.time_zone = Some("UTC".into());
    config
        .options_mut()
        .sql_parser
        .enable_subquery_sort_elimination = false;
    config.options_mut().sql_parser.collect_spans = true;
    config.options_mut().execution.sort_spill_reservation_bytes =
        (resources.memory_limit / resources.threads / 8).min(10 * 1024 * 1024);
    let state = SessionStateBuilder::new()
        .with_config(config)
        .with_runtime_env(runtime)
        .with_default_features()
        .with_query_planner(Arc::new(planner::unnest::OrderedQueryPlanner))
        .build();
    Ok(SessionContext::new_with_state(state))
}
async fn execute(cli: Cli, run_id: &str) -> Result<()> {
    let (path, resources, run, scan, explain) = match cli.command {
        Command::Check {
            job,
            scan,
            resources,
        } => (job, resources, false, scan, false),
        Command::Explain { job, resources } => (job, resources, false, false, true),
        Command::Run { job, resources } => (job, resources, true, false, false),
    };
    let job = config::load_with_paths(&path, &cli.paths)?;
    let temp = resources
        .temp_dir
        .clone()
        .unwrap_or_else(std::env::temp_dir);
    let temp = if temp.is_absolute() {
        temp
    } else {
        std::env::current_dir()
            .map_err(|e| Error::config(e.to_string()))?
            .join(temp)
    };
    sink::preflight(&job, &temp)?;
    log(
        json!({"event":"start","run_id":run_id,"version":env!("CARGO_PKG_VERSION"),"time":chrono::Utc::now().to_rfc3339(),"job":path,"hashes":job.hashes,"expanded_hash":job.expanded_hash,"threads":resources.threads,"memory_limit":resources.memory_limit}),
    );
    for d in job.input.iter().chain(&job.output) {
        if d.format == config::Format::Parquet {
            continue;
        }
        let enc = crate::encoding::TextEncoding::resolve(&d.encoding)?;
        log(
            json!({"event":"encoding","run_id":run_id,"definition":d.id,"label":d.encoding,"canonical":enc.0.name(),"output_encoding":if enc.0==encoding_rs::UTF_16LE || enc.0==encoding_rs::UTF_16BE {enc.0.name()}else{enc.0.output_encoding().name()}}),
        );
    }
    let ctx = context(&resources, None)?;
    let planned = planner::plan(&ctx, &job).await?;
    // Build every physical plan before a temporary output can be opened.
    for (d, p) in job.output.iter().zip(&planned.outputs) {
        let frame = ctx.execute_logical_plan(p.plan.clone()).await?;
        let physical = frame
            .create_physical_plan()
            .await
            .map_err(|e| Error::from(e).context(&d.id))?;
        if explain {
            println!(
                "output {}\nLogical plan:\n{}\nPhysical plan:\n{}",
                d.id,
                p.plan.display_indent(),
                displayable(physical.as_ref()).indent(true)
            );
        }
    }
    if scan {
        for (source, layout) in &planned.inputs {
            let dataset = source.dataset.clone();
            let layout = layout.clone();
            let count = tokio::task::spawn_blocking(move || -> Result<u64> {
                let mut reader = InputReader::open(&dataset, &layout)?;
                let mut count = 0;
                while let Some(batch) = reader.next_batch(layout.schema.clone(), None)? {
                    count += batch.num_rows() as u64;
                }
                Ok(count)
            })
            .await
            .map_err(|e| Error::sql(e.to_string()))??;
            println!("{}: {count} records scanned", source.dataset.id);
        }
    }
    if !run {
        if !explain {
            println!(
                "OK: {} inputs, {} processes, {} outputs",
                job.input.len(),
                job.process.len(),
                job.output.len()
            );
        }
        return Ok(());
    }
    fs::create_dir_all(&temp).map_err(|e| Error::output(e.to_string()))?;
    let work = tempfile::Builder::new()
        .prefix(&format!("dxtran-{run_id}-"))
        .tempdir_in(&temp)
        .map_err(|e| Error::output(e.to_string()))?;
    let outputs = work.path().join("outputs");
    let spill = work.path().join("spill");
    fs::create_dir(&outputs)
        .and_then(|_| fs::create_dir(&spill))
        .map_err(|e| Error::output(e.to_string()))?;
    let ctx = context(&resources, Some(&spill))?;
    // The validated logical plans contain immutable lazy providers and can execute in this runtime.
    let mut staged = vec![];
    let conversion_budget = planned.inputs[0].0.conversion_budget.clone();
    for (d, p) in job.output.iter().zip(&planned.outputs) {
        let start = Instant::now();
        let file = outputs.join(format!("{}.tmp", d.id));
        let mut sink = sink::OutputSink::create(d, &p.layout, &file, &p.schema)?;
        let physical = ctx
            .execute_logical_plan(p.plan.clone())
            .await?
            .create_physical_plan()
            .await
            .map_err(original_error)?;
        let physical = cooperative::wrap(physical).map_err(original_error)?;
        let mut stream =
            datafusion::physical_plan::execute_stream(physical.clone(), ctx.task_ctx())
                .map_err(original_error)?;
        let workers = crate::parallel::workers(resources.threads, resources.memory_limit);
        if let Some(encoder) = sink.fixed_encoder().filter(|_| workers > 1) {
            sink = fixed::write(sink, stream, encoder, conversion_budget.clone(), workers)
                .await
                .map_err(|e| e.context(format!("output {}", d.id)))?;
        } else if let Some(encoder) = sink.csv_encoder().filter(|_| workers > 1) {
            sink = csv::write(sink, stream, encoder, conversion_budget.clone(), workers)
                .await
                .map_err(|e| e.context(format!("output {}", d.id)))?;
        } else {
            while let Some(batch) = stream.next().await {
                let batch =
                    batch.map_err(|e| original_error(e).context(format!("output {}", d.id)))?;
                let permit = conversion_budget
                    .clone()
                    .acquire_owned()
                    .await
                    .map_err(|e| Error::sql(e.to_string()))?;
                sink = tokio::task::spawn_blocking(move || -> Result<sink::OutputSink> {
                    sink.batch(batch)?;
                    drop(permit);
                    Ok(sink)
                })
                .await
                .map_err(|e| Error::output(e.to_string()))??;
            }
        }
        let rows = tokio::task::spawn_blocking(move || sink.finish())
            .await
            .map_err(|e| Error::output(e.to_string()))??;
        let bytes = fs::metadata(&file)
            .map_err(|e| Error::output(e.to_string()))?
            .len();
        fn spill_metrics(
            plan: &Arc<dyn datafusion::physical_plan::ExecutionPlan>,
        ) -> (usize, usize) {
            let metrics = plan.metrics();
            let mut count = metrics.as_ref().and_then(|m| m.spill_count()).unwrap_or(0);
            let mut bytes = metrics
                .as_ref()
                .and_then(|m| m.spilled_bytes())
                .unwrap_or(0);
            for child in plan.children() {
                let (c, b) = spill_metrics(child);
                count += c;
                bytes += b;
            }
            (count, bytes)
        }
        let (spill_count, spilled_bytes) = spill_metrics(&physical);
        log(
            json!({"event":"output_staged","run_id":run_id,"output":d.id,"path":d.path,"rows":rows,"bytes":bytes,"spill_count":spill_count,"spilled_bytes":spilled_bytes,"seconds":start.elapsed().as_secs_f64()}),
        );
        staged.push(file);
    }
    sink::publish(&job, &staged)?;
    // Drop DataFusion's disk manager before removing this run's working directory.
    drop(ctx);
    work.close().map_err(|e| Error::output(e.to_string()))?;
    Ok(())
}
pub fn main_cli() -> i32 {
    let cli = match parse_cli(std::env::args_os()) {
        Ok(c) => c,
        Err(e) => {
            let code = if e.use_stderr() { 2 } else { 0 };
            let _ = e.print();
            return code;
        }
    };
    let threads = match &cli.command {
        Command::Check { resources, .. }
        | Command::Explain { resources, .. }
        | Command::Run { resources, .. } => resources.threads,
    };
    let run_id = format!(
        "{}-{}",
        chrono::Utc::now().format("%Y%m%dT%H%M%S%.9f"),
        std::process::id()
    );
    let start = Instant::now();
    let runtime = match tokio::runtime::Builder::new_multi_thread()
        .worker_threads(threads)
        .max_blocking_threads(threads)
        .enable_all()
        .build()
    {
        Ok(r) => r,
        Err(e) => {
            eprintln!("{e}");
            return 4;
        }
    };
    let (signals, signal_rx) = match signals::Signals::new() {
        Ok(s) => s,
        Err(e) => {
            eprintln!("{e}");
            return 4;
        }
    };
    let result=runtime.block_on(async {tokio::select! {biased;_=signal_rx=>Err(Error::sql("interrupted")),result=execute(cli,&run_id)=>result}});
    drop(signals);
    let code = match result {
        Ok(()) => 0,
        Err(e) => {
            log(json!({"event":"error","run_id":run_id,"phase":phase(e.code),"message":e.message}));
            i32::from(e.code)
        }
    };
    runtime.shutdown_timeout(std::time::Duration::from_secs(5));
    log(
        json!({"event":"finish","run_id":run_id,"time":chrono::Utc::now().to_rfc3339(),"seconds":start.elapsed().as_secs_f64(),"exit_code":code}),
    );
    code
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn defaults() {
        assert_eq!(
            (1..=10).map(default_threads_for).collect::<Vec<_>>(),
            [1, 1, 1, 2, 2, 3, 3, 4, 4, 4]
        );
        assert_eq!(parse_memory("1GiB").unwrap(), 1 << 30);
        assert!(positive("0").is_err());
        assert!(parse_memory("0MiB").is_err());
    }
    #[test]
    fn computation_errors_do_not_log_input_values() {
        let error = original_error(datafusion::error::DataFusionError::Execution(
            "Cannot cast secret-input-42 to integer".into(),
        ));
        assert!(!error.message.contains("secret-input-42"));
        assert!(error.message.contains("type conversion"));
    }
}
