-module(lethe_sup).

-behavior(supervisor).

-export([
    start_link/0,
    init/1
]).


start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).


init([]) ->
    Worker = {
        lethe_server,
        {lethe_server, start_link, []},
        permanent,
        100,
        worker,
        [lethe_server]
    },
    {ok, {{one_for_one, 5, 10}, [Worker]}}.

