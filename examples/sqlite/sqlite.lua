-- SQLite external tables and transactional outputs for embedded LuaJIT.
local ffi = require("ffi")
ffi.cdef[[
typedef struct sqlite3 sqlite3;
typedef struct sqlite3_stmt sqlite3_stmt;
int sqlite3_open_v2(const char *, sqlite3 **, int, const char *);
int sqlite3_close_v2(sqlite3 *);
const char *sqlite3_errmsg(sqlite3 *);
int sqlite3_prepare_v2(sqlite3 *, const char *, int, sqlite3_stmt **, const char **);
int sqlite3_stmt_readonly(sqlite3_stmt *);
int sqlite3_step(sqlite3_stmt *);
int sqlite3_finalize(sqlite3_stmt *);
int sqlite3_column_count(sqlite3_stmt *);
const char *sqlite3_column_name(sqlite3_stmt *, int);
int sqlite3_column_type(sqlite3_stmt *, int);
double sqlite3_column_double(sqlite3_stmt *, int);
const unsigned char *sqlite3_column_text(sqlite3_stmt *, int);
const void *sqlite3_column_blob(sqlite3_stmt *, int);
int sqlite3_column_bytes(sqlite3_stmt *, int);
int sqlite3_busy_timeout(sqlite3 *, int);
int sqlite3_exec(sqlite3 *, const char *, int (*)(void *, int, char **, char **), void *, char **);
int sqlite3_bind_null(sqlite3_stmt *, int);
int sqlite3_bind_int(sqlite3_stmt *, int, int);
int sqlite3_bind_int64(sqlite3_stmt *, int, int64_t);
int sqlite3_bind_double(sqlite3_stmt *, int, double);
int sqlite3_bind_text(sqlite3_stmt *, int, const char *, int, void (*)(void *));
int sqlite3_bind_blob(sqlite3_stmt *, int, const void *, int, void (*)(void *));
int sqlite3_clear_bindings(sqlite3_stmt *);
int sqlite3_reset(sqlite3_stmt *);
]]

local plugin = {
    open = function(options)
        -- The YAML entry may be a loader name or an explicit .so/.dylib/.dll path.
        local lib = dxt.load_library("sqlite3")
        local db, stmt
        local function close()
            if stmt ~= nil then
                ffi.gc(stmt, nil)
                lib.sqlite3_finalize(stmt)
                stmt = nil
            end
            if db ~= nil then
                ffi.gc(db, nil)
                lib.sqlite3_close_v2(db)
                db = nil
            end
        end
        local function failure(operation, rc)
            local message = db ~= nil and ffi.string(lib.sqlite3_errmsg(db)) or "no connection"
            error(operation .. " (" .. rc .. "): " .. message)
        end
        -- open() must clean up its own partial resources if it fails.
        local ok, cursor = pcall(function()
            local handle = ffi.new("sqlite3 *[1]")
            local rc = lib.sqlite3_open_v2(dxt.resolve_path(assert(options.database)), handle, 1, nil)
            if handle[0] ~= nil then
                db = ffi.gc(handle[0], function(p) lib.sqlite3_close_v2(p) end)
            end
            if rc ~= 0 then failure("sqlite3_open_v2", rc) end
            local query = assert(options.query)
            assert(not query:find("\0", 1, true), "query contains NUL")
            local statement = ffi.new("sqlite3_stmt *[1]")
            local tail = ffi.new("const char *[1]")
            rc = lib.sqlite3_prepare_v2(db, query, #query + 1, statement, tail)
            if statement[0] ~= nil then
                stmt = ffi.gc(statement[0], function(p) lib.sqlite3_finalize(p) end)
            end
            if rc ~= 0 then failure("sqlite3_prepare_v2", rc) end
            assert(stmt ~= nil, "query must contain a statement")
            assert(ffi.string(tail[0]):match("^%s*$"), "query must contain exactly one statement")
            assert(lib.sqlite3_stmt_readonly(stmt) ~= 0, "query must be read-only")
            local count = lib.sqlite3_column_count(stmt)
            assert(count > 0, "query must return columns")
            local names, seen = {}, {}
            for i = 0, count - 1 do
                local name = ffi.string(lib.sqlite3_column_name(stmt, i))
                assert(not seen[name], "duplicate result column: " .. name)
                names[i + 1], seen[name] = name, true
            end
            return {
                next = function()
                    if stmt == nil then return nil end
                    local step = lib.sqlite3_step(stmt)
                    if step == 101 then return nil end -- SQLITE_DONE
                    if step ~= 100 then failure("sqlite3_step", step) end -- SQLITE_ROW
                    local row = {}
                    for i = 0, count - 1 do
                        local kind = lib.sqlite3_column_type(stmt, i)
                        local value
                        if kind == 2 then -- REAL
                            value = lib.sqlite3_column_double(stmt, i)
                        elseif kind == 4 then -- BLOB (including empty blobs)
                            local ptr = lib.sqlite3_column_blob(stmt, i)
                            local size = lib.sqlite3_column_bytes(stmt, i)
                            assert(size == 0 or ptr ~= nil, "sqlite3_column_blob failed")
                            value = size == 0 and "" or ffi.string(ptr, size)
                        elseif kind ~= 5 then -- INTEGER or TEXT; NULL stays nil
                            -- Reading INTEGER as text preserves all 64 bits.
                            local ptr = lib.sqlite3_column_text(stmt, i)
                            local size = lib.sqlite3_column_bytes(stmt, i)
                            assert(ptr ~= nil, "sqlite3_column_text failed")
                            value = ffi.string(ptr, size)
                        end
                        row[names[i + 1]] = value
                    end
                    return row
                end,
                close = close,
            }
        end)
        if not ok then close(); error(cursor) end
        return cursor
    end,
}

local function identifier(name)
    return type(name) == "string" and name:match("^[a-z][a-z0-9_]*$") ~= nil
end

function plugin.validate_output(options, columns, mode)
    assert(type(options.database) == "string" and #options.database > 0, "database is required")
    assert(not options.database:find("\0", 1, true), "database contains NUL")
    assert(identifier(options.table), "table must be a public identifier")
    assert(mode == "append" or mode == "overwrite" or mode == "merge", "invalid output mode")
    local names = {}
    for _, column in ipairs(columns) do names[column.name] = true end
    if mode == "merge" then
        assert(type(options.keys) == "table" and #options.keys > 0, "merge requires keys")
        local seen = {}
        for _, key in ipairs(options.keys) do
            assert(names[key], "merge key is not in the SQL output: " .. tostring(key))
            assert(not seen[key], "duplicate merge key: " .. key)
            seen[key] = true
        end
    end
    local timeout = options.busy_timeout_ms or 5000
    assert(type(timeout) == "number" and timeout >= 0 and timeout <= 2147483647
        and timeout % 1 == 0, "busy_timeout_ms must be a nonnegative integer")
end

-- Parse decimal digits using cdata arithmetic; tonumber() would lose 64-bit precision.
local function int64(value)
    local negative = value:sub(1, 1) == "-"
    local magnitude = 0ULL
    for i = negative and 2 or 1, #value do
        magnitude = magnitude * 10ULL + (value:byte(i) - 48)
    end
    local limit = negative and 9223372036854775808ULL or 9223372036854775807ULL
    assert(magnitude <= limit, "integer is outside SQLite's signed 64-bit range")
    return ffi.cast("int64_t", negative and (0ULL - magnitude) or magnitude)
end

function plugin.open_writer(options, columns, mode)
    plugin.validate_output(options, columns, mode)
    local lib = dxt.load_library("sqlite3")
    local db, stmt
    local active = false
    local function failure(operation, rc)
        local message = db ~= nil and ffi.string(lib.sqlite3_errmsg(db)) or "no connection"
        error(operation .. " (" .. rc .. "): " .. message)
    end
    local function checked(operation, rc)
        if rc ~= 0 then failure(operation, rc) end
    end
    local function exec(sql)
        checked(sql, lib.sqlite3_exec(db, sql, nil, nil, nil))
    end
    local function finalize()
        if stmt ~= nil then
            ffi.gc(stmt, nil)
            lib.sqlite3_finalize(stmt)
            stmt = nil
        end
    end
    local function rollback()
        if active then
            finalize()
            exec("ROLLBACK")
            active = false
        end
    end
    local function close()
        if db ~= nil then
            finalize()
            -- Closing an uncommitted SQLite connection also rolls it back.
            ffi.gc(db, nil)
            lib.sqlite3_close_v2(db)
            db = nil
            active = false
        end
    end
    local ok, writer = pcall(function()
        local handle = ffi.new("sqlite3 *[1]")
        -- READWRITE, without CREATE: an incorrect database path must fail.
        local rc = lib.sqlite3_open_v2(dxt.resolve_path(options.database), handle, 2, nil)
        if handle[0] ~= nil then
            db = ffi.gc(handle[0], function(p) lib.sqlite3_close_v2(p) end)
        end
        if rc ~= 0 then failure("sqlite3_open_v2", rc) end
        checked("sqlite3_busy_timeout", lib.sqlite3_busy_timeout(db, options.busy_timeout_ms or 5000))
        exec("PRAGMA foreign_keys = ON")
        exec("BEGIN IMMEDIATE")
        active = true
        local function quoted(name) return '"' .. name .. '"' end
        local names, placeholders = {}, {}
        for i, column in ipairs(columns) do
            names[i], placeholders[i] = quoted(column.name), "?"
        end
        local sql = "INSERT INTO " .. quoted(options.table) .. " (" .. table.concat(names, ",")
            .. ") VALUES (" .. table.concat(placeholders, ",") .. ")"
        if mode == "merge" then
            local keys, key_set, updates = {}, {}, {}
            for i, key in ipairs(options.keys) do keys[i], key_set[key] = quoted(key), true end
            for _, column in ipairs(columns) do
                if not key_set[column.name] then
                    local name = quoted(column.name)
                    updates[#updates + 1] = name .. "=excluded." .. name
                end
            end
            sql = sql .. " ON CONFLICT (" .. table.concat(keys, ",") .. ") DO "
                .. (#updates == 0 and "NOTHING" or "UPDATE SET " .. table.concat(updates, ","))
        end
        local statement = ffi.new("sqlite3_stmt *[1]")
        rc = lib.sqlite3_prepare_v2(db, sql, #sql + 1, statement, nil)
        if statement[0] ~= nil then
            stmt = ffi.gc(statement[0], function(p) lib.sqlite3_finalize(p) end)
        end
        if rc ~= 0 then failure("sqlite3_prepare_v2", rc) end
        if mode == "overwrite" then exec("DELETE FROM " .. quoted(options.table)) end
        local transient = ffi.cast("void (*)(void *)", -1)
        return {
            write = function(row)
                if mode == "merge" then
                    for _, key in ipairs(options.keys) do
                        assert(row[key] ~= nil, "NULL merge key: " .. key)
                    end
                end
                for i, column in ipairs(columns) do
                    local value = row[column.name]
                    local bound
                    if value == nil then
                        bound = lib.sqlite3_bind_null(stmt, i)
                    elseif column.type == "int64" or column.type == "uint64" then
                        bound = lib.sqlite3_bind_int64(stmt, i, int64(value))
                    elseif column.type == "float64" then
                        bound = lib.sqlite3_bind_double(stmt, i, value)
                    elseif column.type == "boolean" then
                        bound = lib.sqlite3_bind_int(stmt, i, value and 1 or 0)
                    elseif column.type == "binary" then
                        bound = lib.sqlite3_bind_blob(stmt, i, value, #value, transient)
                    else
                        bound = lib.sqlite3_bind_text(stmt, i, value, #value, transient)
                    end
                    checked("sqlite3_bind", bound)
                end
                local step = lib.sqlite3_step(stmt)
                if step ~= 101 then failure("sqlite3_step", step) end
                checked("sqlite3_reset", lib.sqlite3_reset(stmt))
                checked("sqlite3_clear_bindings", lib.sqlite3_clear_bindings(stmt))
            end,
            commit = function()
                finalize()
                exec("COMMIT")
                active = false
            end,
            rollback = rollback,
            close = close,
        }
    end)
    if not ok then
        pcall(rollback)
        close()
        error(writer)
    end
    return writer
end

return plugin
