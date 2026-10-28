return {
    open = function(options)
        local row = 0
        return {
            next = function()
                row = row + 1
                if row > 2 then return nil end
                dxt.job.read_rows = dxt.job.read_rows + 1
                return {id = row, label = dxt.job.label}
            end,
        }
    end,
    open_writer = function(options, columns, mode)
        local count = 0
        return {
            write = function(row) count = count + 1 end,
            commit = function() dxt.job.saved_rows = dxt.job.saved_rows + count end,
            rollback = function() count = 0 end,
        }
    end,
}
