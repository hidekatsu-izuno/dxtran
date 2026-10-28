//! Common trusted LuaJIT environment for input and output plugins.
mod state;
use crate::config::{Dataset, Lua as Settings};
use mlua::{ChunkMode, Function, Lua, LuaSerdeExt, Table, Value};
pub(crate) use state::{JobState, Resource, Runtime};
use std::{
    collections::BTreeMap,
    fs,
    io::{ErrorKind, Read},
    path::{Path, PathBuf},
    sync::Arc,
};

const MAX_SCRIPT_BYTES: usize = 1024 * 1024;

fn read_script(path: &Path) -> mlua::Result<Vec<u8>> {
    let file = fs::File::open(path).map_err(mlua::Error::external)?;
    if !file.metadata().map_err(mlua::Error::external)?.is_file() {
        return Err(mlua::Error::runtime("Lua source must be a regular file"));
    }
    let mut source = Vec::new();
    file.take(MAX_SCRIPT_BYTES as u64 + 1)
        .read_to_end(&mut source)
        .map_err(mlua::Error::external)?;
    if source.len() > MAX_SCRIPT_BYTES {
        return Err(mlua::Error::runtime("Lua source exceeds 1 MiB"));
    }
    Ok(source)
}

// LuaJIT FFI deliberately allows native calls. The same trust boundary applies
// as loading a native plugin: scripts and their libraries must be trusted.
#[allow(unsafe_code)]
fn plugin_vm() -> Lua {
    // SAFETY: This API explicitly executes trusted plugin code with native FFI.
    // No untrusted data is evaluated as Lua; callers supply the script path.
    unsafe { Lua::unsafe_new() }
}

pub(crate) struct Plugin {
    pub lua: Lua,
    pub module: Table,
    pub options: Table,
    pub job: Option<Arc<JobState>>,
}

pub(crate) fn load(d: &Dataset) -> mlua::Result<Plugin> {
    let config = d.lua.clone().unwrap_or_default();
    if !config.allow_unsafe {
        return Err(mlua::Error::runtime(
            "Lua execution is disabled; explicitly allow unsafe Lua only for trusted plugin code",
        ));
    }
    if let Some(job) = &config.job {
        return job.load(d);
    }
    let lua = plugin_vm();
    let module = module(&lua, &d.path, &config, None)?;
    let options = options(&lua, &config.options)?;
    Ok(Plugin {
        lua,
        module,
        options,
        job: None,
    })
}

pub(super) fn options(
    lua: &Lua,
    values: &BTreeMap<String, serde_json::Value>,
) -> mlua::Result<Table> {
    let Value::Table(options) = lua.to_value(values)? else {
        unreachable!()
    };
    Ok(options)
}

pub(super) fn base(path: &Path, config: &Settings) -> PathBuf {
    if config.base_dir.as_os_str().is_empty() {
        path.parent().unwrap_or(Path::new(".")).to_path_buf()
    } else {
        config.base_dir.clone()
    }
}

pub(super) fn module(
    lua: &Lua,
    path: &Path,
    config: &Settings,
    variables: Option<&Table>,
) -> mlua::Result<Table> {
    let base = base(path, config);
    let api = lua.create_table()?;
    if let Some(variables) = variables {
        api.set("job", variables.clone())?;
    }
    api.set(
        "resolve_path",
        lua.create_function(move |_, path: String| {
            base.join(path)
                .into_os_string()
                .into_string()
                .map_err(|_| mlua::Error::runtime("plugin path must be UTF-8"))
        })?,
    )?;
    let libraries = config.libraries.clone();
    api.set(
        "load_library",
        lua.create_function(move |lua, name: String| {
            let library = libraries.get(&name).ok_or_else(|| {
                mlua::Error::runtime(format!("lua.libraries has no entry '{name}'"))
            })?;
            let ffi: Table = lua.load("return require('ffi')").eval()?;
            let ffi_load: Function = ffi.get("load")?;
            ffi_load.call::<Value>(library.as_str())
        })?,
    )?;
    // Each plugin gets its own globals and path helpers, even in a shared VM.
    let env = lua.create_table()?;
    let meta = lua.create_table()?;
    meta.set("__index", lua.globals())?;
    env.set_metatable(Some(meta))?;
    env.set("_G", env.clone())?;
    env.set("dxt", api)?;
    let directory =
        fs::canonicalize(path.parent().unwrap_or(Path::new("."))).map_err(mlua::Error::external)?;
    let helper = lua.create_function(move |lua, (name, env): (String, Table)| {
        if !name.split('.').all(|part| {
            !part.is_empty() && part.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_')
        }) {
            return Err(mlua::Error::runtime(
                "Lua module name must contain only dotted identifiers",
            ));
        }
        let relative = name.replace('.', std::path::MAIN_SEPARATOR_STR);
        for candidate in [
            directory.join(format!("{relative}.lua")),
            directory.join(relative).join("init.lua"),
        ] {
            let resolved = match fs::canonicalize(&candidate) {
                Ok(path) => path,
                Err(error) if error.kind() == ErrorKind::NotFound => continue,
                Err(error) => return Err(mlua::Error::external(error)),
            };
            if !resolved.starts_with(&directory) {
                return Err(mlua::Error::runtime(
                    "Lua helper escapes the plugin directory",
                ));
            }
            let source = read_script(&resolved)?;
            let function = lua
                .load(source)
                .set_mode(ChunkMode::Text)
                .set_name(resolved.to_string_lossy())
                .set_environment(env.clone())
                .into_function()?;
            return Ok(Some(function));
        }
        Ok(None)
    })?;
    let native_require: Function = lua.globals().get("require")?;
    let require: Function = lua
        .load(
            r#"
        local env, helper, native = ...
        local loaded, loading = {}, {}
        return function(name)
            if loaded[name] == loading then error('cyclic Lua module: ' .. name) end
            if loaded[name] then return loaded[name] end
            if package.loaded[name] then return package.loaded[name] end
            if package.preload[name] then return native(name) end
            local chunk = helper(name, env)
            if not chunk then error('Lua module not found in plugin directory: ' .. tostring(name)) end
            loaded[name] = loading
            local ok, value = pcall(chunk, name)
            if not ok then loaded[name] = nil; error(value) end
            if value == nil then value = true end
            loaded[name] = value
            return value
        end
    "#,
        )
        .call((
            env.clone(),
            helper,
            native_require,
        ))?;
    env.set("require", require)?;
    let script = read_script(path)?;
    lua.load(script)
        .set_mode(ChunkMode::Text)
        .set_name(path.to_string_lossy())
        .set_environment(env)
        .eval()
}
