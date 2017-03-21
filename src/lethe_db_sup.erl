-module(lethe_db_sup).

-behavior(supervisor).

-export([
    start_link/0,
    init/1
]).


start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).


init([]) ->
    Worker = {
        lethe_db,
        {lethe_db, start_link, []},
        transient,
        100,
        worker,
        [lethe_db]
    },
    {ok, {{simple_one_for_one, 5, 10}, [Worker]}}.

