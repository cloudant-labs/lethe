-module(lethe_server).

-behavior(gen_server).


-export([
    start_link/0,
    db_exists/1,
    open_db/2,
    delete_db/1
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
-include_lib("lethe/include/lethe.hrl").


-define(DBS_TABLE, lethe_dbs).


-record(st, {}).


start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).


db_exists(Name) ->
    ets:member(?DBS_TABLE, Name).


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
                [#lethe_db{} = Db] ->
                    {ok, Db};
                [] ->
                    throw({not_found, no_db_file})
            end
    end.


delete_db(Name) ->
    gen_server:call(?MODULE, {delete_db, Name}).


init([]) ->
    ?DBS_TABLE = ets:new(?DBS_TABLE, [{keypos, #lethe_db.name}, protected, set, named_table, {read_concurrency, true}]),
    {ok, #st{}}.


handle_call({create_db, Name, Options}, _From, St) ->
    {reply, create_db_int(Name, Options), St};
handle_call({delete_db, Name}, _From, St) ->
    {reply, delete_db_int(Name), St};
handle_call(Msg, _From, St) ->
    {reply, {error, unknown_msg, Msg}, St}.


handle_cast(_Msg, St) ->
    {noreply, St}.


handle_info(_Msg, St) ->
    {noreply, St}.


terminate(_Reason, _St) ->
    true = ets:delete(?DBS_TABLE),
    ok.


code_change(_OldVsn, #st{} = State, _Extra) ->
    {ok, State}.


create_db_int(Name, Options) ->
    {ok, Pid} = supervisor:start_child(lethe_db_sup, [Name, Options]),
    Db = #lethe_db{} = lethe_db:get_db(Pid),
    %% TODO: handle the case when this fails, for instance,
    %% when multiple messages in the queue to create a db
    true = ets:insert_new(?DBS_TABLE, Db),

    {ok, Db}.


delete_db_int(Name) ->
    case ets:lookup(?DBS_TABLE, Name) of
        [#lethe_db{} = Db] ->
            true = ets:delete(?DBS_TABLE, Db#lethe_db.name),
            ok = supervisor:terminate_child(lethe_db_sup, Db#lethe_db.pid);
        [] ->
            {error, not_found}
    end.

