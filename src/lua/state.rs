//! Shared job variables, isolated plugin modules, and ordered cleanup.
use super::{Plugin, base, module, options, plugin_vm};
use crate::{
    config::{self, Dataset},
    error::{Error, Result},
};
use mlua::{Function, Lua, Table};
use std::{
    collections::BTreeMap,
    path::PathBuf,
    sync::{
        Arc, Mutex, MutexGuard, Weak,
        atomic::{AtomicBool, Ordering},
    },
};

pub(crate) struct Resource {
    pub table: Table,
    writer: bool,
    closed: AtomicBool,
    committed: AtomicBool,
}
impl Resource {
    pub fn new(table: Table, writer: bool) -> Arc<Self> {
        Arc::new(Self {
            table,
            writer,
            closed: AtomicBool::new(false),
            committed: AtomicBool::new(false),
        })
    }
    pub fn committed(&self) -> bool {
        self.committed.load(Ordering::Acquire)
    }
    pub fn mark_committed(&self) {
        self.committed.store(true, Ordering::Release);
    }
    pub fn close(&self) -> mlua::Result<()> {
        if self.closed.swap(true, Ordering::AcqRel) {
            return Ok(());
        }
        let mut errors = Vec::new();
        for (name, required) in if self.writer && !self.committed() {
            vec![("rollback", true), ("close", false)]
        } else {
            vec![("close", false)]
        } {
            let result = (|| -> mlua::Result<()> {
                let function: Option<Function> = self.table.raw_get(name)?;
                match function {
                    Some(function) => function.call(()),
                    None if !required => Ok(()),
                    None => Err(mlua::Error::runtime(format!("missing writer.{name}"))),
                }
            })();
            if let Err(error) = result {
                errors.push(format!("{name}: {error}"));
            }
        }
        if errors.is_empty() {
            Ok(())
        } else {
            Err(mlua::Error::runtime(errors.join("; ")))
        }
    }
}

type ModuleKey = (PathBuf, PathBuf, BTreeMap<String, String>);
struct State {
    lua: Lua,
    variables: Table,
    options: Table,
    init: Option<Function>,
    close: Option<Function>,
    started: bool,
    init_error: Option<String>,
    modules: BTreeMap<ModuleKey, Table>,
}

pub(crate) struct JobState {
    definition: config::JobLua,
    gate: Mutex<()>,
    state: Mutex<Option<State>>,
    resources: Mutex<Vec<Weak<Resource>>>,
    closing: AtomicBool,
    allowed: AtomicBool,
}
impl std::fmt::Debug for JobState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LuaJobState")
            .field("path", &self.definition.path)
            .finish_non_exhaustive()
    }
}
fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}
impl JobState {
    fn new(definition: config::JobLua) -> Self {
        Self {
            definition,
            gate: Mutex::new(()),
            state: Mutex::new(None),
            resources: Mutex::new(vec![]),
            closing: AtomicBool::new(false),
            allowed: AtomicBool::new(false),
        }
    }
    pub fn lock(&self) -> mlua::Result<MutexGuard<'_, ()>> {
        let guard = lock(&self.gate);
        if self.closing.load(Ordering::Acquire) {
            return Err(mlua::Error::runtime("Lua job state is closed"));
        }
        Ok(guard)
    }
    pub fn cleanup_lock(&self) -> MutexGuard<'_, ()> {
        lock(&self.gate)
    }
    fn initialize(&self, state: &mut Option<State>) -> mlua::Result<()> {
        if !self.allowed.load(Ordering::Acquire) {
            return Err(mlua::Error::runtime(
                "job Lua execution is disabled; explicit operator authorization is required",
            ));
        }
        if state.is_some() {
            return Ok(());
        }
        let lua = plugin_vm();
        let variables = options(&lua, &self.definition.variables)?;
        let settings = config::Lua {
            libraries: self.definition.libraries.clone(),
            base_dir: self.definition.base_dir.clone(),
            ..Default::default()
        };
        let mut modules = BTreeMap::new();
        let (init, close) = if let Some(path) = &self.definition.path {
            let plugin = module(&lua, path, &settings, Some(&variables))?;
            let init = plugin.raw_get("init")?;
            let close = plugin.raw_get("close")?;
            let key = (path.clone(), base(path, &settings), settings.libraries);
            modules.insert(key, plugin);
            (init, close)
        } else {
            (None, None)
        };
        let options = options(&lua, &self.definition.options)?;
        *state = Some(State {
            lua,
            variables,
            options,
            init,
            close,
            started: false,
            init_error: None,
            modules,
        });
        Ok(())
    }
    fn validate(&self) -> Result<()> {
        let _guard = self.lock().map_err(|e| Error::config(e.to_string()))?;
        self.initialize(&mut lock(&self.state))
            .map_err(|e| Error::config(e.to_string()).context("job lua"))
    }
    pub fn load(self: &Arc<Self>, dataset: &Dataset) -> mlua::Result<Plugin> {
        let _guard = self.lock()?;
        let mut state = lock(&self.state);
        self.initialize(&mut state)?;
        let state = state.as_mut().unwrap();
        let mut settings = dataset.lua.clone().unwrap_or_default();
        let mut libraries = self.definition.libraries.clone();
        libraries.extend(settings.libraries);
        settings.libraries = libraries;
        let key = (
            dataset.path.clone(),
            base(&dataset.path, &settings),
            settings.libraries.clone(),
        );
        let plugin = if let Some(plugin) = state.modules.get(&key) {
            plugin.clone()
        } else {
            let plugin = module(&state.lua, &dataset.path, &settings, Some(&state.variables))?;
            state.modules.insert(key, plugin.clone());
            plugin
        };
        Ok(Plugin {
            lua: state.lua.clone(),
            module: plugin,
            options: options(&state.lua, &settings.options)?,
            job: Some(self.clone()),
        })
    }
    // Caller holds the gate. Initialization runs once, before any data callbacks.
    pub fn start_locked(&self) -> mlua::Result<()> {
        let mut state = lock(&self.state);
        self.initialize(&mut state)?;
        let state = state.as_mut().unwrap();
        if let Some(error) = &state.init_error {
            return Err(mlua::Error::runtime(error.clone()));
        }
        if state.started {
            return Ok(());
        }
        state.started = true;
        if let Some(init) = &state.init
            && let Err(error) = init.call::<()>((state.variables.clone(), state.options.clone()))
        {
            state.init_error = Some(error.to_string());
            return Err(error);
        }
        Ok(())
    }
    pub fn register(&self, resource: &Arc<Resource>) {
        let mut resources = lock(&self.resources);
        resources.retain(|resource| resource.strong_count() > 0);
        resources.push(Arc::downgrade(resource));
    }
    fn close(&self, success: bool) -> Result<()> {
        // Stop queued data callbacks, then wait for the current native call.
        self.closing.store(true, Ordering::Release);
        let _guard = self.cleanup_lock();
        let resources = std::mem::take(&mut *lock(&self.resources));
        let mut errors = Vec::new();
        for resource in resources.into_iter().rev().filter_map(|r| r.upgrade()) {
            if let Err(error) = resource.close() {
                errors.push(error.to_string());
            }
        }
        let close = lock(&self.state).as_mut().and_then(|state| {
            if !state.started {
                return None;
            }
            state.started = false;
            state
                .close
                .clone()
                .map(|close| (close, state.variables.clone()))
        });
        if let Some((close, variables)) = close
            && let Err(error) = close.call::<()>((variables, success && errors.is_empty()))
        {
            errors.push(error.to_string());
        }
        if errors.is_empty() {
            Ok(())
        } else {
            Err(Error::output(errors.join("; ")).context("job lua cleanup failed"))
        }
    }
}
impl Drop for JobState {
    fn drop(&mut self) {
        let _ = self.close(false);
    }
}

#[derive(Clone, Debug)]
pub(crate) struct Runtime {
    state: Option<Arc<JobState>>,
}
impl Runtime {
    pub fn allow_unsafe_lua(&self) {
        if let Some(state) = &self.state {
            state.allowed.store(true, Ordering::Release);
        }
    }
    pub fn is_allowed(&self) -> bool {
        self.state
            .as_ref()
            .is_none_or(|state| state.allowed.load(Ordering::Acquire))
    }
    pub fn new(definition: Option<&config::JobLua>) -> Self {
        Self {
            state: definition.map(|definition| Arc::new(JobState::new(definition.clone()))),
        }
    }
    pub fn state(&self) -> Option<Arc<JobState>> {
        self.state.clone()
    }
    pub fn validate(&self) -> Result<()> {
        if let Some(state) = &self.state {
            state.validate()?;
        }
        Ok(())
    }
    pub fn start(&self) -> Result<()> {
        if let Some(state) = &self.state {
            let _guard = state.lock().map_err(|e| Error::config(e.to_string()))?;
            state
                .start_locked()
                .map_err(|e| Error::input(e.to_string()).context("job lua.init"))?;
        }
        Ok(())
    }
    pub fn guard(&self) -> Guard {
        Guard {
            runtime: self.clone(),
            finished: false,
        }
    }
    fn close(&self, success: bool) -> Result<()> {
        if let Some(state) = &self.state {
            state.close(success)?;
        }
        Ok(())
    }
}
pub(crate) struct Guard {
    runtime: Runtime,
    finished: bool,
}
impl Guard {
    pub fn finish<T>(mut self, result: Result<T>) -> Result<T> {
        self.finished = true;
        let cleanup = self.runtime.close(result.is_ok());
        match (result, cleanup) {
            (Ok(value), Ok(())) => Ok(value),
            (Ok(_), Err(error)) | (Err(error), Ok(())) => Err(error),
            (Err(error), Err(cleanup)) => Err(error.context(cleanup)),
        }
    }
}
impl Drop for Guard {
    fn drop(&mut self) {
        if !self.finished
            && let Err(error) = self.runtime.close(false)
        {
            eprintln!(
                "{}",
                serde_json::json!({"event":"job_lua_cleanup_error", "message":error.message})
            );
        }
    }
}
