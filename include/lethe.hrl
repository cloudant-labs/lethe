-record(lethe_db, {
    name,
    body_tab,
    fdi_tab,
    local_tab,
    seq_tab,
    pid,
    curr_seq, %% used by compaction
    monitor
}).


