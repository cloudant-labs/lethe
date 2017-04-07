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
-include_lib("lethe/include/lethe.hrl").


-include_lib("eunit/include/eunit.hrl").

-record(st, {
    header,
    name,
    fdi_tab,
    body_tab,
    local_tab,
    lethe_db,
    monitor
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
        monitor = lethe_db:incref(Db)
    }}.


terminate(_Reason, #st{}) ->
    ok.


delete(_RootDir, Name, _Async) ->
    case exists(Name) of
        true -> lethe_server:delete_db(Name);
        false -> {error, not_found}
    end.


get_compacted_seq(#st{lethe_db=Db}) -> lethe_db:get_compacted_seq(Db).
get_del_doc_count(#st{lethe_db=Db}) -> lethe_db:get_del_doc_count(Db).
get_disk_version(#st{lethe_db=Db}) -> lethe_db:get_disk_version(Db).
get_doc_count(#st{lethe_db=Db}) -> lethe_db:get_doc_count(Db).
get_epochs(#st{lethe_db=Db}) -> lethe_db:get_epochs(Db).
get_last_purged(#st{lethe_db=Db}) -> lethe_db:get_last_purged(Db).
get_purge_seq(#st{lethe_db=Db}) -> lethe_db:get_purge_seq(Db).
get_revs_limit(#st{lethe_db=Db}) -> lethe_db:get_revs_limit(Db).
get_security(#st{lethe_db=Db}) -> lethe_db:get_security(Db).
get_size_info(#st{lethe_db=Db}) -> lethe_db:get_size_info(Db).
get_update_seq(#st{lethe_db=Db}) -> lethe_db:get_update_seq(Db).
get_uuid(#st{lethe_db=Db}) -> lethe_db:get_uuid(Db).


set_revs_limit(#st{lethe_db=Db} = St, Value) ->
    ok = lethe_db:set_revs_limit(Db, Value),
    {ok, St}.


set_security(#st{lethe_db=Db} = St, Value) ->
    ok = lethe_db:set_security(Db, Value),
    {ok, St}.


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


open_local_docs(#st{local_tab = Tab}, DocIds) ->
    open_docs_int(Tab, DocIds).


write_doc_body(#st{lethe_db = Db} = St, #doc{} = Doc) ->
    lethe_db:write_doc_body(Db, Doc).


write_doc_infos(#st{} = St, Pairs, LocalDocs, PurgeInfo) ->
    #st{lethe_db = Db} = St,
    ok = lethe_db:write_doc_infos(Db, Pairs, LocalDocs, PurgeInfo),
    {ok, St}.


serialize_doc(#st{}, #doc{} = Doc) ->
    Doc#doc{body = ?term_to_bin({Doc#doc.body, Doc#doc.atts})}.


commit_data(St) ->
    {ok, St}.


read_doc_body(#st{} = St, #doc{id=Id} = Doc) ->
    case ets:lookup(St#st.body_tab, Doc#doc.body) of
        [] -> not_found;
        [{{_Id, _Rev}, BodyTerm}] ->
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


count_changes_since(#st{lethe_db=Db}, SinceSeq) ->
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
    {ok, Pid} = lethe_db:start_compaction(Db, Options, Parent),
    Db1 = Db#lethe_db{curr_seq=UpdateSeq},
    {ok, St#st{lethe_db=Db1}, Pid}.


finish_compaction(#st{lethe_db=Db} = St, DbName, Options, _Info) ->
    UpdateSeqStart = Db#lethe_db.curr_seq,
    UpdateSeqCurr = get_update_seq(St),
    case UpdateSeqStart == UpdateSeqCurr of
        true ->
            {ok, St, undefined};
        false ->
            start_compaction(St, DbName, Options, self())
    end.


monitored_by(#st{lethe_db=Db}=_St) ->
    lethe_db:monitored_by(Db).


incref(#st{lethe_db=Db}=St) ->
    Ref = lethe_db:incref(Db),
    {ok, St#st{monitor=Ref}}.


decref(#st{monitor=Monitor}) ->
    true = lethe_db:decref(Monitor),
    ok.


%% placeholders
delete_compaction_files(_RootDir, _DirPath, _DelOpts) -> throw(not_implemented).
handle_call(_Msg, _St) -> throw(not_implemented).
handle_info(_E, _St) -> throw(not_implemented).


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

