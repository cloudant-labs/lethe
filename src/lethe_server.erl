-module(lethe_server).

-behavior(gen_server).


-export([
    start_link/0,
    db_exists/1,
    open_db/2,
    delete_db/1
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

    incref/1,
    decref/1,
    monitored_by/1
]).


-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3
]).


-include_lib("couch/include/couch_db.hrl").
-include_lib("stdlib/include/ms_transform.hrl").


-define(DBS_TABLE, lethe_dbs).


-record(st, {}).

-record(lethe_db, {
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
    security = dso,
    size_info = [],
    update_seq = 0,
    uuid = couch_uuids:random(),
    compression = couch_compress:get_compression_method()
}).


start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).


db_exists(Name) ->
    case ets:lookup(?DBS_TABLE, Name) of
        [#lethe_db{}] -> true;
        [] -> false
    end.


open_db(Name, Options) ->
    case lists:member(create, Options) of
        true ->
            case db_exists(Name) of
                true ->
                    throw({error, eexist});
                false ->
                    gen_server:call(?MODULE, {create_db, Name, Options})
            end;
        false ->
            case ets:lookup(?DBS_TABLE, Name) of
                [#lethe_db{}=Db] ->
                    {ok, Db};
                [] ->
                    throw({not_found, no_db_file})
            end
    end.


delete_db(Name) ->
    gen_server:call(?MODULE, {delete_db, Name}).


inc_update_seq(#lethe_db{}=Db) ->
    inc_update_seq(Db, 1).


inc_update_seq(#lethe_db{name=Name}, Count) when Count >= 0 ->
    Res = ets:update_counter(?DBS_TABLE, Name, {#lethe_db.update_seq, Count}),
    Res.


get_doc_count(#lethe_db{fdi_tab=Tab}) ->
    MS = ets:fun2ms(fun(FDI=#full_doc_info{deleted=false}) -> true end),
    ets:select_count(Tab, MS).


get_del_doc_count(#lethe_db{fdi_tab=Tab}) ->
    MS = ets:fun2ms(fun(FDI=#full_doc_info{deleted=true}) -> true end),
    ets:select_count(Tab, MS).


get_update_seq(#lethe_db{name = Name}) ->
    case ets:lookup(?DBS_TABLE, Name) of
        [] ->
            0;
        [#lethe_db{update_seq = UpdateSeq}] ->
            UpdateSeq
    end.


get_purge_seq(#lethe_db{name = Name}) ->
    case ets:lookup(?DBS_TABLE, Name) of
        [] ->
            0;
        [#lethe_db{purge_seq = PurgeSeq}] ->
            PurgeSeq
    end.


get_last_purged(#lethe_db{name = Name}) ->
    case ets:lookup(?DBS_TABLE, Name) of
        [] ->
            0;
        [#lethe_db{last_purged=LastPurged}] ->
            LastPurged
    end.


get_compacted_seq(#lethe_db{compacted_seq = CompactedSeq}) -> CompactedSeq.
get_disk_version(#lethe_db{disk_version = DiskVersion}) -> DiskVersion.
get_epochs(#lethe_db{epochs = Epochs}) -> Epochs.
get_revs_limit(#lethe_db{revs_limit = RevsLimit}) -> RevsLimit.
get_security(#lethe_db{security = Security}) -> Security.
get_size_info(#lethe_db{size_info = SizeInfo}) -> SizeInfo.
get_uuid(#lethe_db{uuid = UUID}) -> UUID.


set_revs_limit(#lethe_db{}=Db, Value) ->
    gen_server:call(?MODULE, {set_revs_limit, Db, Value}).


set_security(#lethe_db{}=Db, Value) ->
    gen_server:call(?MODULE, {set_security, Db, Value}).


purge_docs(#lethe_db{}=Db, PurgeInfo) ->
    gen_server:call(?MODULE, {purge_docs, Db, PurgeInfo}).


start_compaction(#lethe_db{}=Db, Options, Parent) ->
    gen_server:call(?MODULE, {start_compaction, Db, Options, Parent}).


incref(#lethe_db{}=Db) ->
    erlang:monitor(process, ?MODULE).


decref(Ref) ->
    true = erlang:demonitor(Ref, [flush]).


monitored_by(#lethe_db{}) ->
    case erlang:process_info(whereis(?MODULE), monitored_by) of
        {monitored_by, Pids} ->
            lists:usort(Pids);
        _ ->
            []
    end.


init([]) ->
    ?DBS_TABLE = ets:new(?DBS_TABLE, [{keypos, #lethe_db.name}, public, set, named_table]),
    {ok, #st{}}.


handle_call({create_db, Name, Options}, _From, St) ->
    {reply, create_db_int(Name, Options), St};
handle_call({delete_db, Name}, _From, St) ->
    {reply, delete_db_int(Name), St};
handle_call({set_revs_limit, #lethe_db{}=Db, Value}, _From, St) ->
    {reply, set_revs_limit_int(Db, Value), St};
handle_call({set_security, #lethe_db{}=Db, Value}, _From, St) ->
    {reply, set_security_int(Db, Value), St};
handle_call({purge_docs, #lethe_db{}=Db, PurgeInfo}, _From, St) ->
    {reply, purge_docs_int(Db, PurgeInfo), St};
handle_call({start_compaction, #lethe_db{}=Db, Options, Parent}, From, St) ->
    gen_server:reply(From, {ok, self()}),
    ok = compact_db(Db, Options),
    gen_server:cast(Parent, {compact_done, lethe, Db}),
    {noreply, St};
handle_call(Msg, _From, St) ->
    {reply, {error, unknown_msg, Msg}, St}.


handle_cast(_Msg, St) ->
    {noreply, St}.


handle_info(_Msg, St) ->
    {noreply, St}.


terminate(_Reason, _St) ->
    true = ets:delete(?DBS_TABLE),
    ok.


code_change(_OldVsn, #st{}=State, _Extra) ->
    {ok, State}.


create_db_int(Name, Options) ->
    BodyTab = ets:new(body_tab, [set, public]),
    FDITab = ets:new(fdi_tab, [{keypos, #full_doc_info.id}, ordered_set, public]),
    LocalTab = ets:new(local_tab, [{keypos, 2}, ordered_set, public]),
    SeqTab = ets:new(seq_tab, [ordered_set, public]),
    Db = #lethe_db{
        name = Name,
        body_tab = BodyTab,
        fdi_tab = FDITab,
        local_tab = LocalTab,
        seq_tab = SeqTab
    },
    %% TODO: handle the case when this fails, for instance,
    %% when multiple messages in the queue to create a db
    true = ets:insert_new(?DBS_TABLE, Db),

    ok = maybe_track(Options),
    {ok, Db}.


delete_db_int(Name) ->
    case ets:lookup(?DBS_TABLE, Name) of
        [#lethe_db{}=Db] ->
            true = ets:delete(Db#lethe_db.fdi_tab),
            true = ets:delete(Db#lethe_db.body_tab),
            true = ets:delete(Db#lethe_db.local_tab),
            true = ets:delete(Db#lethe_db.seq_tab),
            true = ets:delete(?DBS_TABLE, Db#lethe_db.name),
            ok;
        [] ->
            {error, not_found}
    end.


set_revs_limit_int(#lethe_db{name=Name}=Db0, RevsLimit) ->
    Db = Db0#lethe_db{revs_limit=RevsLimit},
    true = ets:update_element(?DBS_TABLE, Name, {#lethe_db.revs_limit, RevsLimit}),
    {ok, Db}.


set_security_int(#lethe_db{name=Name}=Db0, SecurityProps) ->
    Db = Db0#lethe_db{security=SecurityProps},
    true = ets:update_element(?DBS_TABLE, Name, {#lethe_db.security, SecurityProps}),
    {ok, Db}.


purge_docs_int(#lethe_db{name=Name}=Db0, PurgeInfo) ->
    Db = Db0#lethe_db{
        purge_seq = Db0#lethe_db.purge_seq + 1,
        last_purged = PurgeInfo
    },
    true = ets:update_element(?DBS_TABLE, Name, {#lethe_db.last_purged, PurgeInfo}),
    _ = ets:update_counter(?DBS_TABLE, Name, {#lethe_db.purge_seq, 1}),
    {ok, Db}.


compact_db(#lethe_db{fdi_tab=FDITab}=Db, Options) ->
    compact_db(Db, ets:first(FDITab), Options).


compact_db(_Db, '$end_of_table', _Options) ->
    ok;
compact_db(Db, Key, Options) ->
    #lethe_db{
        fdi_tab = FDITab,
        body_tab = BodyTab,
        revs_limit = RevsLimit
    } = Db,
    [FDI] = ets:lookup(FDITab, Key),
    #full_doc_info{id = Id, rev_tree = RevTree} = FDI,
    {NewRevTree, _} = couch_key_tree:mapfold(fun compact_doc/4, {BodyTab, []}, RevTree),
    NewRevTree1 = couch_key_tree:stem(NewRevTree, RevsLimit),
    true = ets:update_element(FDITab, Id, {#full_doc_info.rev_tree, NewRevTree1}),
    compact_db(Db, ets:next(FDITab, Id), Options).


compact_doc(Rev, Leaf=#leaf{ptr=Ptr}, branch, {Tab,_}=Acc) ->
    Res = ets:lookup(Tab, Ptr),
    {?REV_MISSING, Acc};
compact_doc(Rev, Leaf, leaf, Acc) ->
    {Leaf, Acc}.


maybe_track(Options) ->
    IsSys = lists:member(sys_db, Options),
    io:format("MAYBE_TRACK OPTIONS[~p]: ~p~n", [IsSys, Options]),
    if IsSys -> ok; true ->
            couch_stats_process_tracker:track([lethe, open_lethes])
    end,
    ok.

