//! DataFusion runtime construction.
use super::Resources;
use crate::{error::Result, planner};
use datafusion::{
    execution::{
        disk_manager::{DiskManagerBuilder, DiskManagerMode},
        memory_pool::FairSpillPool,
        runtime_env::RuntimeEnvBuilder,
        session_state::SessionStateBuilder,
    },
    prelude::{SessionConfig, SessionContext},
};
use std::{path::Path, sync::Arc};

pub(super) fn context(resources: &Resources, spill: Option<&Path>) -> Result<SessionContext> {
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
    let context = SessionContext::new_with_state(state);
    crate::functions::register(&context);
    Ok(context)
}
