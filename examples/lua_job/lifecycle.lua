-- Optional hooks; lua.variables alone is enough to share ordinary values.
return {
    init = function(job, options)
        job.summary_path = dxt.resolve_path(options.summary_path)
    end,
    close = function(job, success)
        local file = assert(io.open(job.summary_path, "wb"))
        file:write("label=" .. job.label .. "\n")
        file:write("read_rows=" .. job.read_rows .. "\n")
        file:write("saved_rows=" .. job.saved_rows .. "\n")
        file:write("success=" .. tostring(success) .. "\n")
        file:close()
    end,
}
