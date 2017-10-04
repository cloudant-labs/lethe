-module(lethe_db).

-behavior(gen_server).


-export([
    start_link/2,
    get_db/1
]).


-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3
]).


-export([
    get_compacted_seq/1,
    get_del_doc_count/1,
    get_disk_version/1,
    get_doc_count/1,
    get_epochs/1,
    get_last_purged/1,
    get_purge_seq/1,
    get_revs_limit/1,
    get_security/1,
    get_size_info/1,
    get_update_seq/1,
    get_uuid/1,

    set_revs_limit/2,
    set_security/2
]).


-export([
    inc_update_seq/1,
    inc_update_seq/2,

    purge_docs/2,

    start_compaction/3,

    write_doc_body/2,
    write_doc_infos/4,

    incref/1,
    decref/1,
    monitored_by/1
]).


-include_lib("stdlib/include/ms_transform.hrl").
-include_lib("couch/include/couch_db.hrl").
-include_lib("lethe/include/lethe.hrl").


-record(st, {
    name,
    body_tab,
    fdi_tab,
    local_tab,
    seq_tab,
    compacted_seq = 0,
    del_doc_count = 0,
    disk_version = 1,
    doc_count = 0,
    epochs = [{node(), 0}],
    last_purged = [],
    purge_seq = 0,
    revs_limit = 1000,
    security = [],
    size_info = [
        {active, 0},
        {external, 0},
        {file, 0}
    ],
    update_seq = 0,
    uuid = couch_uuids:random(),
    compression = couch_compress:get_compression_method()
}).


-record(wiacc, {
    new_ids = [],
    rem_ids = [],
    new_seqs = [],
    rem_seqs = [],
    update_seq
}).


start_link(Name, Options) ->
    gen_server:start_link(?MODULE, [Name, Options], []).


get_doc_count(#lethe_db{fdi_tab = Tab}) ->
    MS = ets:fun2ms(fun(#full_doc_info{deleted=false}) -> true end),
    ets:select_count(Tab, MS).


get_del_doc_count(#lethe_db{fdi_tab = Tab}) ->
    MS = ets:fun2ms(fun(#full_doc_info{deleted=true}) -> true end),
    ets:select_count(Tab, MS).


get_update_seq(#lethe_db{pid = Pid}) -> gen_server:call(Pid, get_update_seq).
get_purge_seq(#lethe_db{pid = Pid}) -> gen_server:call(Pid, get_purge_seq).
get_last_purged(#lethe_db{pid = Pid}) -> gen_server:call(Pid, get_last_purged).
get_compacted_seq(#lethe_db{pid = Pid}) -> gen_server:call(Pid, get_compacted_seq).
get_disk_version(#lethe_db{pid = Pid}) -> gen_server:call(Pid, get_disk_version).
get_epochs(#lethe_db{pid = Pid}) -> gen_server:call(Pid, get_epochs).
get_revs_limit(#lethe_db{pid = Pid}) -> gen_server:call(Pid, get_revs_limit).
get_security(#lethe_db{pid = Pid}) -> gen_server:call(Pid, get_security).
get_size_info(#lethe_db{pid = Pid}) -> gen_server:call(Pid, get_size_info).
get_uuid(#lethe_db{pid = Pid}) -> gen_server:call(Pid, get_uuid).


set_revs_limit(#lethe_db{pid = Pid}, Value) ->
    gen_server:call(Pid, {set_revs_limit, Value}).


set_security(#lethe_db{pid = Pid}, Value) ->
    gen_server:call(Pid, {set_security, Value}).


inc_update_seq(#lethe_db{} = Db) ->
    inc_update_seq(Db, 1).


inc_update_seq(#lethe_db{pid = Pid}, Count) when Count >= 0 ->
    gen_server:call(Pid, {inc_update_seq, Count}).


purge_docs(#lethe_db{pid = Pid}, PurgeInfo) ->
    gen_server:call(Pid, {purge_docs, PurgeInfo}).


start_compaction(#lethe_db{pid = Pid}, Options, Parent) ->
    gen_server:call(Pid, {start_compaction, Options, Parent}).


get_db(Pid) ->
    gen_server:call(Pid, get_db).


write_doc_body(#lethe_db{pid = Pid}, #doc{} = Doc) ->
    gen_server:call(Pid, {write_doc_body, Doc}).


write_doc_infos(#lethe_db{pid = Pid}, Pairs, LocalDocs, PurgeInfo) ->
    gen_server:call(Pid, {write_doc_infos, Pairs, LocalDocs, PurgeInfo}).


incref(#lethe_db{pid = Pid} = Db) ->
    Ref = erlang:monitor(process, Pid),
    Db#lethe_db{monitor = Ref}.


decref(Ref) ->
    true = erlang:demonitor(Ref, [flush]).


monitored_by(#lethe_db{pid = Pid}) ->
    case erlang:process_info(Pid, monitored_by) of
        {monitored_by, Pids} ->
            lists:usort(Pids);
        _ ->
            []
    end.


init([Name, Options]) ->
    BodyTab = ets:new(body_tab, [set, protected, {read_concurrency, true}]),
    FDITab = ets:new(fdi_tab, [{keypos, #full_doc_info.id}, ordered_set, protected, {read_concurrency, true}]),
    LocalTab = ets:new(local_tab, [{keypos, 2}, ordered_set, protected, {read_concurrency, true}]),
    SeqTab = ets:new(seq_tab, [ordered_set, protected, {read_concurrency, true}]),
    St = #st{
        name = Name,
        body_tab = BodyTab,
        fdi_tab = FDITab,
        local_tab = LocalTab,
        seq_tab = SeqTab
    },

    ok = maybe_track(Options),

    {ok, St}.


handle_call(get_db, _From, St) ->
    {reply, make_db(St), St};
handle_call({purge_docs, PurgeInfo}, _From, St) ->
    St1 = purge_docs_int(St, PurgeInfo),
    {reply, ok, St1};
handle_call({start_compaction, Options, Parent}, From, St) ->
    gen_server:reply(From, {ok, self()}),
    ok = compact_db(St, Options),
    gen_server:cast(Parent, {compact_done, lethe, ok}),
    {noreply, St};
handle_call({inc_update_seq, Count}, _From, St) ->
    #st{update_seq = UpdateSeq} = St,
    {reply, ok, St#st{update_seq = UpdateSeq + Count}};
handle_call({set_revs_limit, Value}, _From, St) ->
    {reply, ok, St#st{revs_limit = Value}};
handle_call({set_security, Value}, _From, St) ->
    {reply, ok, St#st{security = Value}};
handle_call(get_update_seq, _From, St) ->
    {reply, St#st.update_seq, St};
handle_call(get_purge_seq, _From, St) ->
    {reply, St#st.purge_seq, St};
handle_call(get_last_purged, _From, St) ->
    {reply, St#st.last_purged, St};
handle_call(get_compacted_seq, _From, St) ->
    {reply, St#st.compacted_seq, St};
handle_call(get_disk_version, _From, St) ->
    {reply, St#st.disk_version, St};
handle_call(get_epochs, _From, St) ->
    {reply, St#st.epochs, St};
handle_call(get_revs_limit, _From, St) ->
    {reply, St#st.revs_limit, St};
handle_call(get_security, _From, St) ->
    {reply, St#st.security, St};
handle_call(get_size_info, _From, St) ->
    {reply, St#st.size_info, St};
handle_call(get_uuid, _From, St) ->
    {reply, St#st.uuid, St};
handle_call({write_doc_body, Doc}, _From, St) ->
    #st{body_tab = Tab} = St,
    {reply, write_doc_body_int(Tab, Doc), St};
handle_call({write_doc_infos, Pairs, LocalDocs, PurgeInfo}, _From, St) ->
    {ok, St1} = write_doc_infos_int(St, Pairs, LocalDocs, PurgeInfo),
    {reply, ok, St1};
handle_call(Msg, _From, St) ->
    {reply, {error, unknown_msg, Msg}, St}.


handle_cast(_Msg, St) ->
    {noreply, St}.


handle_info(_Msg, St) ->
    {noreply, St}.


terminate(_Reason, St) ->
    %% TODO: lethe_server:drop_db(St#st.name)
    true = ets:delete(St#st.fdi_tab),
    true = ets:delete(St#st.body_tab),
    true = ets:delete(St#st.local_tab),
    true = ets:delete(St#st.seq_tab),
    ok.


code_change(_OldVsn, #st{}=State, _Extra) ->
    {ok, State}.


make_db(St) ->
    #st{
        name = Name,
        body_tab = BodyTab,
        fdi_tab = FDITab,
        local_tab = LocalTab,
        seq_tab = SeqTab
    } = St,
    #lethe_db{
        name = Name,
        body_tab = BodyTab,
        fdi_tab = FDITab,
        local_tab = LocalTab,
        seq_tab = SeqTab,
        pid = self()
    }.


maybe_track(Options) ->
    IsSys = lists:member(sys_db, Options),
    if IsSys -> ok; true ->
        %% TODO: enable
        %% couch_stats_process_tracker:track([lethe, open_lethes])
        ok
    end,
    ok.


purge_docs_int(St, PurgeInfo) ->
    St#st{
        purge_seq = St#st.purge_seq + 1,
        last_purged = PurgeInfo
    }.


compact_db(#st{fdi_tab = FDITab}=St, Options) ->
    compact_db(St, ets:first(FDITab), Options).


compact_db(_St, '$end_of_table', _Options) ->
    ok;
compact_db(St, Key, Options) ->
    #st{
        fdi_tab = FDITab,
        body_tab = BodyTab,
        revs_limit = RevsLimit
    } = St,
    [FDI] = ets:lookup(FDITab, Key),
    #full_doc_info{id = Id, rev_tree = RevTree} = FDI,
    {NewRevTree, _} = couch_key_tree:mapfold(fun compact_doc/4, {BodyTab, []}, RevTree),
    NewRevTree1 = couch_key_tree:stem(NewRevTree, RevsLimit),
    true = ets:update_element(FDITab, Id, {#full_doc_info.rev_tree, NewRevTree1}),
    compact_db(St, ets:next(FDITab, Id), Options).


compact_doc(_Rev, #leaf{}, branch, Acc) ->
    %%Res = ets:lookup(Tab, Ptr),
    %% TODO: actually delete docs
    {?REV_MISSING, Acc};
compact_doc(_Rev, Leaf, leaf, Acc) ->
    {Leaf, Acc}.


write_doc_body_int(Tab, #doc{} = Doc) ->
    #doc{
        id = Id,
        revs = {Start, [FirstRevId|_]}
    } = Doc,
    Rev = ?l2b([integer_to_list(Start),"-",?l2b(couch_util:to_hex(FirstRevId))]),
    true = ets:insert_new(Tab, [{{Id, Rev}, Doc#doc.body}]),
    {ok, Doc#doc{body={Id, Rev}}, size(Doc#doc.body)}.


write_doc_infos_int(#st{} = St0, Pairs, LocalDocs, PurgeInfo) ->
    #st{
        update_seq = UpdateSeq,
        seq_tab = SeqTab,
        fdi_tab = FDITab
    } = St0,
    #wiacc{
        %% new_ids = NewIds,
        rem_ids = RemIds,
        new_seqs = NewSeqs,
        rem_seqs = RemSeqs,
        update_seq = NewSeq0
    } = get_write_info(St0, Pairs),

    {NewSeq, St1} = case PurgeInfo of
        [] ->
            {NewSeq0, St0};
        _ ->
            {NewSeq0 + 1, purge_docs_int(St0, PurgeInfo)}
    end,

    SeqDelta = NewSeq - UpdateSeq,
    St2 = St1#st{update_seq = UpdateSeq + SeqDelta},
    [true = ets:delete(SeqTab, K) || K <- RemSeqs],
    true = ets:insert_new(SeqTab, NewSeqs),

    %% FIXME
    [true = ets:delete(FDITab, K) || K <- RemIds],

    ok = write_local_doc_infos(St2, LocalDocs),

    {ok, St2}.


get_write_info(St, Pairs) ->
    Acc = #wiacc{update_seq = St#st.update_seq},
    get_write_info(St, Pairs, Acc).


get_write_info(_St, [], Acc) ->
    Acc;
get_write_info(St, [{OldFDI, NewFDI} | Rest], Acc) ->
    NewAcc = case {OldFDI, NewFDI} of
        {not_found, #full_doc_info{}} ->
            #full_doc_info{
                id = Id,
                update_seq = Seq
            } = NewFDI,
            {ok, Ptr} = write_doc_info(St, NewFDI),
            Acc#wiacc{
                new_ids = [{Id, Ptr} | Acc#wiacc.new_ids],
                new_seqs = [{Seq, Ptr} | Acc#wiacc.new_seqs],
                update_seq = erlang:max(Seq, Acc#wiacc.update_seq)
            };
        {#full_doc_info{id = Id}, #full_doc_info{id = Id}} ->
            #full_doc_info{
                update_seq = OldSeq
            } = OldFDI,
            #full_doc_info{
                update_seq = NewSeq
            } = NewFDI,
            {ok, Ptr} = write_doc_info(St, NewFDI),
            Acc#wiacc{
                new_ids = [{Id, Ptr} | Acc#wiacc.new_ids],
                new_seqs = [{NewSeq, Ptr} | Acc#wiacc.new_seqs],
                rem_seqs = [OldSeq | Acc#wiacc.rem_seqs],
                update_seq = erlang:max(NewSeq, Acc#wiacc.update_seq)
            };
        {#full_doc_info{}, not_found} ->
            #full_doc_info{
                id = Id,
                update_seq = Seq
            } = OldFDI,
            Acc#wiacc{
                rem_ids = [Id | Acc#wiacc.rem_ids],
                rem_seqs = [Seq | Acc#wiacc.rem_seqs]
            }
    end,
    get_write_info(St, Rest, NewAcc).


write_local_doc_infos(#st{}, []) ->
    ok;
write_local_doc_infos(#st{local_tab=Tab}, LocalDocs) ->
    true = ets:insert(Tab, LocalDocs),
    ok.


write_doc_info(St, FDI) ->
    #full_doc_info{
        id = Id,
        update_seq = Seq
    } = FDI,
    true = ets:insert(St#st.fdi_tab, [FDI]),
    {ok, {Id, Seq}}.


