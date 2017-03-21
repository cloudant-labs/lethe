% Licensed under the Apache License, Version 2.0 (the "License"); you may not
% use this file except in compliance with the License. You may obtain a copy of
% the License at
%
%   http://www.apache.org/licenses/LICENSE-2.0
%
% Unless required by applicable law or agreed to in writing, software
% distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
% WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
% License for the specific language governing permissions and limitations under
% the License.

-module(lethe).
-behavior(couch_db_engine).

-export([
    exists/1,

    delete/3,
    delete_compaction_files/3,

    init/2,
    terminate/2,
    handle_call/2,
    handle_info/2,

    incref/1,
    decref/1,
    monitored_by/1,

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
    set_security/2,

    open_docs/2,
    open_local_docs/2,
    read_doc_body/2,

    serialize_doc/2,
    write_doc_body/2,
    write_doc_infos/4,

    commit_data/1,

    open_write_stream/2,
    open_read_stream/2,
    is_active_stream/2,

    fold_docs/4,
    fold_local_docs/4,
    fold_changes/5,
    count_changes_since/2,

    start_compaction/4,
    finish_compaction/4
]).


-include_lib("couch/include/couch_db.hrl").


-include_lib("eunit/include/eunit.hrl").

-record(wiacc, {
    new_ids = [],
    rem_ids = [],
    new_seqs = [],
    rem_seqs = [],
    update_seq
}).


-record(st, {
    header,
    name,
    fdi_tab,
    body_tab,
    local_tab,
    lethe_db,
    monitor
}).

%% TODO: FIXME: make this an opaque API or move to shared .hrl
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


exists(Name) ->
    lethe_server:db_exists(Name).


init(Name, Options) ->
    {ok, Db} = lethe_server:open_db(Name, Options),
    {ok, #st{
        name = Name,
        lethe_db = Db,
        fdi_tab = Db#lethe_db.fdi_tab,
        body_tab = Db#lethe_db.body_tab,
        local_tab = Db#lethe_db.local_tab,
        monitor = lethe_server:incref(Db)
    }}.


terminate(_Reason, #st{}) ->
    ok.


delete(_RootDir, Name, _Async) ->
    case exists(Name) of
        true -> lethe_server:delete_db(Name);
        false -> {error, not_found}
    end.


get_compacted_seq(#st{lethe_db=Db}) -> lethe_server:get_compacted_seq(Db).
get_del_doc_count(#st{lethe_db=Db}) -> lethe_server:get_del_doc_count(Db).
get_disk_version(#st{lethe_db=Db}) -> lethe_server:get_disk_version(Db).
get_doc_count(#st{lethe_db=Db}) -> lethe_server:get_doc_count(Db).
get_epochs(#st{lethe_db=Db}) -> lethe_server:get_epochs(Db).
get_last_purged(#st{lethe_db=Db}) -> lethe_server:get_last_purged(Db).
get_purge_seq(#st{lethe_db=Db}) -> lethe_server:get_purge_seq(Db).
get_revs_limit(#st{lethe_db=Db}) -> lethe_server:get_revs_limit(Db).
get_security(#st{lethe_db=Db}) -> lethe_server:get_security(Db).
get_size_info(#st{lethe_db=Db}) -> lethe_server:get_size_info(Db).
get_update_seq(#st{lethe_db=Db}) -> lethe_server:get_update_seq(Db).
get_uuid(#st{lethe_db=Db}) -> lethe_server:get_uuid(Db).


set_revs_limit(#st{lethe_db=Db} = St, Value) ->
    {ok, Db1} = lethe_server:set_revs_limit(Db, Value),
    {ok, St#st{lethe_db=Db1}}.


set_security(#st{lethe_db=Db} = St, Value) ->
    {ok, Db1} = lethe_server:set_security(Db, Value),
    {ok, St#st{lethe_db=Db1}}.


open_docs(#st{fdi_tab=Tab}, DocIds) ->
    open_docs_int(Tab, DocIds).


open_docs_int(Tab, DocIds) ->
    lists:map(fun(DocId) ->
        case ets:lookup(Tab, DocId) of
            [] ->
                not_found;
            [#doc{id=(<<"_local/", _/binary>>), deleted=true}] ->
                  not_found;
            [#doc{id=(<<"_local/", _/binary>>), deleted=false}=Doc] ->
                Doc;
            [#full_doc_info{}=FDI] ->
                FDI
        end
    end, DocIds).


open_local_docs(#st{local_tab=Tab}, DocIds) ->
    open_docs_int(Tab, DocIds).


write_doc_body(#st{body_tab=Tab}, #doc{} = Doc) ->
    #doc{
        id=Id,
        revs={Start, [FirstRevId|_]}
    } = Doc,
    Rev = ?l2b([integer_to_list(Start),"-",?l2b(couch_util:to_hex(FirstRevId))]),
    true = ets:insert_new(Tab, [{{Id, Rev}, Doc#doc.body}]),
    {ok, Doc#doc{body={Id, Rev}}, size(Doc#doc.body)}.


serialize_doc(#st{}, #doc{} = Doc) ->
    Doc#doc{body = ?term_to_bin({Doc#doc.body, Doc#doc.atts})}.


commit_data(St) ->
    {ok, St}.


read_doc_body(#st{} = St, #doc{id=Id} = Doc) ->
    case ets:lookup(St#st.body_tab, Doc#doc.body) of
        [] -> not_found;
        [{{Id, _Rev}, BodyTerm}] ->
            {Body, Atts} = binary_to_term(BodyTerm),
            Doc#doc{
                body = Body,
                atts = Atts
            }
    end.


open_write_stream(_, _) -> throw(not_supported).
open_read_stream(_, _) -> throw(not_supported).
is_active_stream(_, _) -> false.


fold_docs(#st{fdi_tab=Tab}, UserFun, UserAcc, Options) ->
    fold_docs_int(Tab, UserFun, UserAcc, Options).


fold_local_docs(#st{local_tab=Tab}, UserFun, UserAcc, Options) ->
    %%?debugFmt("LOCAL TAB INFO: ~p~n", [ets:info(Tab)]),
    %%?debugFmt("LOCAL OPTS: ~p~n", [Options]),
    fold_docs_int(Tab, UserFun, UserAcc, Options).


fold_docs_int(Tab, UserFun0, UserAcc, Options) ->
    StartKey = couch_util:get_value(start_key, Options),
    EndKey0 = couch_util:get_value(end_key, Options),
    EndKeyGt0 = couch_util:get_value(end_key_gt, Options),
    EndKey = case {EndKey0, EndKeyGt0} of
        {undefined, undefined} -> undefined;
        {EndKey0, undefined} -> EndKey0;
        {undefined, EndKeyGt0} -> {end_key_gt, EndKeyGt0}
    end,
    Dir = couch_util:get_value(dir, Options, fwd),
    UserFun = maybe_wrap_user_fun(UserFun0, Options),

    WrapFun = case {StartKey, EndKey, Dir} of
        {undefined, undefined, _} ->
            fun
                (_, {stop, _}=FullAcc) -> FullAcc;
                (E, {ok, Acc}) -> UserFun(E, Acc)
            end;
        {StartKey, undefined, fwd} ->
            fun
                (_, {stop, _}=FullAcc) -> FullAcc;
                (#full_doc_info{id=Id}, {ok, _}=FullAcc) when Id < StartKey -> FullAcc;
                (#doc{id=Id}, {ok, _}=FullAcc) when Id < StartKey -> FullAcc;
                (E, {ok, Acc}) -> UserFun(E, Acc)
            end;
        {StartKey, undefined, rev} ->
            fun
                (_, {stop, _}=FullAcc) -> FullAcc;
                (#full_doc_info{id=Id}, {ok, _}=FullAcc) when Id > StartKey -> FullAcc;
                (#doc{id=Id}, {ok, _}=FullAcc) when Id > StartKey -> FullAcc;
                (E, {ok, Acc}) -> UserFun(E, Acc)
            end;
        {undefined, {end_key_gt, EndKeyGt}, fwd} ->
            fun
                (_, {stop, _}=FullAcc) -> FullAcc;
                (#full_doc_info{id=Id}, {ok, Acc}) when Id >= EndKeyGt -> {ok, Acc};
                (#doc{id=Id}, {ok, Acc}) when Id >= EndKeyGt -> {ok, Acc};
                (E, {ok, Acc}) -> UserFun(E, Acc)
            end;
        {undefined, {end_key_gt, EndKeyGt}, rev} ->
            fun
                (_, {stop, _}=FullAcc) -> FullAcc;
                (#full_doc_info{id=Id}, {ok, Acc}) when Id =< EndKeyGt -> {ok, Acc};
                (#doc{id=Id}, {ok, Acc}) when Id =< EndKeyGt -> {ok, Acc};
                (E, {ok, Acc}) -> UserFun(E, Acc)
            end;
        {undefined, EndKey, fwd} ->
            fun
                (_, {stop, _}=FullAcc) -> FullAcc;
                (#full_doc_info{id=Id}, {ok, Acc}) when Id > EndKey -> {ok, Acc};
                (#doc{id=Id}, {ok, Acc}) when Id > EndKey -> {ok, Acc};
                (E, {ok, Acc}) -> UserFun(E, Acc)
            end;
        {undefined, EndKey, rev} ->
            fun
                (_, {stop, _}=FullAcc) -> FullAcc;
                (#full_doc_info{id=Id}, {ok, Acc}) when Id < EndKey -> {ok, Acc};
                (#doc{id=Id}, {ok, Acc}) when Id < EndKey -> {ok, Acc};
                (E, {ok, Acc}) -> UserFun(E, Acc)
            end;
        {StartKey, {end_key_gt, EndKeyGt}, fwd} ->
            fun
                (_, {stop, _}=FullAcc) -> FullAcc;
                (#full_doc_info{id=Id}, {ok, _}=FullAcc) when Id < StartKey -> FullAcc;
                (#doc{id=Id}, {ok, _}=FullAcc) when Id < StartKey -> FullAcc;
                (#full_doc_info{id=Id}, {ok, Acc}) when Id > EndKeyGt -> {ok, Acc};
                (#doc{id=Id}, {ok, Acc}) when Id > EndKeyGt -> {ok, Acc};
                (E, {ok, Acc}) -> UserFun(E, Acc)
            end;
        {StartKey, {end_key_gt, EndKeyGt}, rev} ->
            fun
                (_, {stop, _}=FullAcc) -> FullAcc;
                (#full_doc_info{id=Id}, {ok, _}=FullAcc) when Id > StartKey -> FullAcc;
                (#doc{id=Id}, {ok, _}=FullAcc) when Id > StartKey -> FullAcc;
                (#full_doc_info{id=Id}, {ok, Acc}) when Id < EndKeyGt -> {ok, Acc};
                (#doc{id=Id}, {ok, Acc}) when Id < EndKeyGt -> {ok, Acc};
                (E, {ok, Acc}) -> UserFun(E, Acc)
            end;
        {StartKey, EndKey, fwd} ->
            fun
                (_, {stop, _}=FullAcc) -> FullAcc;
                (#full_doc_info{id=Id}, {ok, _}=FullAcc) when Id < StartKey -> FullAcc;
                (#doc{id=Id}, {ok, _}=FullAcc) when Id < StartKey -> FullAcc;
                (#full_doc_info{id=Id}, {ok, Acc}) when Id > EndKey -> {ok, Acc};
                (#doc{id=Id}, {ok, Acc}) when Id > EndKey -> {ok, Acc};
                (E, {ok, Acc}) -> UserFun(E, Acc)
            end;
        {StartKey, EndKey, rev} ->
            fun
                (_, {stop, _}=FullAcc) -> FullAcc;
                (#full_doc_info{id=Id}, {ok, _}=FullAcc) when Id > StartKey -> FullAcc;
                (#doc{id=Id}, {ok, _}=FullAcc) when Id > StartKey -> FullAcc;
                (#full_doc_info{id=Id}, {ok, Acc}) when Id < EndKey -> {ok, Acc};
                (#doc{id=Id}, {ok, Acc}) when Id < EndKey -> {ok, Acc};
                (E, {ok, Acc}) -> UserFun(E, Acc)
            end
    end,

    {_, OutAcc} = case couch_util:get_value(dir, Options, fwd) of
        fwd -> ets:foldl(WrapFun, {ok, UserAcc}, Tab);
        rev -> ets:foldr(WrapFun, {ok, UserAcc}, Tab)
    end,

    wrap_fold_result(OutAcc, Options).


%%count_changes_since(#st{}=St, SinceSeq) ->
count_changes_since(#st{lethe_db=Db}, SinceSeq) ->
    %%UpdateSeq = get_update_seq(St),
    %%UpdateSeq - SinceSeq.
    count_changes_since(Db#lethe_db.seq_tab, SinceSeq, 0).


count_changes_since(Tab, Key, Count) ->
    case ets:next(Tab, Key) of
        '$end_of_table' -> Count;
        NextKey -> count_changes_since(Tab, NextKey, Count + 1)
    end.


fold_changes(#st{lethe_db=Db}, SinceSeq, UserFun, UserAcc, Options) ->
    fold_changes_int(Db, SinceSeq+1, UserFun, {ok, UserAcc}, Options).


fold_changes_int(_Db, '$end_of_table', _Fun, {_, Acc}, _Options) ->
    {ok, Acc};
fold_changes_int(_Db, _, _Fun, {stop, Acc}, _Options) ->
    {ok, Acc};
fold_changes_int(Db, Key, Fun, {ok, Acc0}, Options) ->
    #lethe_db{
        seq_tab = SeqTab,
        fdi_tab = FDITab
    } = Db,
    Acc1 = case ets:lookup(SeqTab, Key) of
        [] ->
            {ok, Acc0};
        [{Key, {Id, _Seq}}] ->
            case open_docs_int(FDITab, [Id]) of
                not_found ->
                    Acc0;
                [FDI] ->
                    Fun(FDI, Acc0)
            end
    end,
    Next = case proplists:get_value(dir, Options, fwd) of
        fwd -> ets:next(SeqTab, Key);
        rev -> ets:prev(SeqTab, Key)
    end,
    fold_changes_int(Db, Next, Fun, Acc1, Options).


start_compaction(#st{lethe_db = Db} = St, _DbName, Options, Parent) ->
    UpdateSeq = get_update_seq(St),
    {ok, Pid} = lethe_server:start_compaction(Db, Options, Parent),
    Db1 = Db#lethe_db{update_seq=UpdateSeq},
    {ok, St#st{lethe_db=Db1}, Pid}.


finish_compaction(#st{lethe_db=Db} = St, DbName, Options, _Info) ->
    UpdateSeqStart = Db#lethe_db.update_seq,
    UpdateSeqCurr = get_update_seq(St),
    case UpdateSeqStart == UpdateSeqCurr of
        true ->
            {ok, St, undefined};
        false ->
            start_compaction(St, DbName, Options, self())
    end.


monitored_by(#st{lethe_db=Db}=_St) ->
    lethe_server:monitored_by(Db).


incref(#st{lethe_db=Db}=St) ->
    Ref = lethe_server:incref(Db),
    {ok, St#st{monitor=Ref}}.


decref(#st{monitor=Monitor}) ->
    true = lethe_server:decref(Monitor),
    ok.


%% placeholders
delete_compaction_files(_RootDir, _DirPath, _DelOpts) -> throw(not_implemented).
handle_call(_Msg, _St) -> throw(not_implemented).
handle_info(_E, _St) -> throw(not_implemented).


write_doc_infos(#st{lethe_db=Db0} = St, Pairs, LocalDocs, PurgeInfo) ->
    UpdateSeq = get_update_seq(St),
    %%io:format("WRITE_DOCS_INFOS -- PAIRS: ~p~n", [Pairs]),
    %%io:format("WRITE_DOCS_INFOS -- LOCAL DOCS: ~p~n", [LocalDocs]),
    #wiacc{
        %% new_ids = NewIds,
        rem_ids = RemIds,
        new_seqs = NewSeqs,
        rem_seqs = RemSeqs,
        update_seq = NewSeq0
    } = get_write_info(St, Pairs),
    %%io:format("WRITE_DOC_INFOS -- NEW IDS: ~p~n", [NewIds]),
    %%io:format("WRITE LOCAL DOCS: ~p~n", [LocalDocs]),
    %%io:format("WRITE_DOC_INFOS -- REM IDS: ~p~n", [RemIds]),
    %io:format("WRITE_DOC_INFOS -- NEW SEQS: ~p~n", [NewSeqs]),
    %%io:format("WRITE_DOC_INFOS -- REM SEQS: ~p~n", [RemSeqs]),
    %%io:format("WRITE_DOC_INFOS -- OLD/NEW UPDATE SEQ: ~p||~p~n", [UpdateSeq, NewSeq]),

    %% IdTree = foo,
    %% SeqTree = bar,
    %% LocalTree = baz,
    %% {ok, IdTree2} = couch_ngen_btree:add_remove(IdTree, NewIds, RemIds),
    %% {ok, SeqTree2} = couch_ngen_btree:add_remove(SeqTree, NewSeqs, RemSeqs),

    %% {AddLDocs, RemLDocIds} = lists:foldl(fun(Doc, {AddAcc, RemAcc}) ->
    %%     case Doc#doc.deleted of
    %%         true ->
    %%             {AddAcc, [Doc#doc.id | RemAcc]};
    %%         false ->
    %%             {[Doc | AddAcc], RemAcc}
    %%     end
    %% end, {[], []}, LocalDocs),
    %% {ok, LocalTree2} = couch_ngen_btree:add_remove(
    %%         LocalTree, AddLDocs, RemLDocIds),

    %% NewHeader = case PurgeInfo of
    %%     [] ->
    %%         couch_ngen_header:set(St#st.header, [
    %%             {update_seq, NewSeq}
    %%         ]);
    %%     _ ->
    %%         {ok, Ptr} = couch_ngen_file:append_term(baaaz, PurgeInfo),
    %%         OldPurgeSeq = couch_ngen_header:get(zaaab, purge_seq),
    %%         couch_ngen_header:set(St#st.header, [
    %%             {update_seq, NewSeq + 1},
    %%             {purge_seq, OldPurgeSeq + 1},
    %%             {purged_docs, Ptr}
    %%         ])
    %% end,

    {NewSeq, Db} = case PurgeInfo of
        [] ->
            {NewSeq0, Db0};
        _ ->
            {ok, Db1} = lethe_server:purge_docs(Db0, PurgeInfo),
            {NewSeq0 + 1, Db1}
    end,

    %%io:format("UPDATING DOC COUNT AND UPDATE SEQ~n", []),
    SeqDelta = NewSeq - UpdateSeq,
    _Old = lethe_server:inc_update_seq(Db, SeqDelta),
    [true = ets:delete(Db#lethe_db.seq_tab, K) || K <- RemSeqs],
    true = ets:insert_new(Db#lethe_db.seq_tab, NewSeqs),

    %% FIXME
    [true = ets:delete(Db#lethe_db.fdi_tab, K) || K <- RemIds],

    ok = write_local_doc_infos(Db, LocalDocs),

    {ok, St}.


get_write_info(St, Pairs) ->
    Acc = #wiacc{update_seq = get_update_seq(St)},
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


write_local_doc_infos(#lethe_db{}, []) ->
    ok;
write_local_doc_infos(#lethe_db{local_tab=Tab}, LocalDocs) ->
    true = ets:insert(Tab, LocalDocs),
    ok.


write_doc_info(St, FDI) ->
    #full_doc_info{
        id = Id,
        update_seq = Seq
    } = FDI,
    true = ets:insert(St#st.fdi_tab, [FDI]),
    {ok, {Id, Seq}}.


maybe_wrap_user_fun(UserFun, Options) ->
    case lists:member(include_reductions, Options) of
        true -> fun(Term, Acc) -> UserFun(Term, {[], []}, Acc) end;
        false -> UserFun
    end.


wrap_fold_result(UserAcc, Options) ->
    case lists:member(include_reductions, Options) of
        true -> {ok, 0, UserAcc};
        false -> {ok, UserAcc}
    end.

