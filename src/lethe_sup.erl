-module(lethe_sup).

-behavior(supervisor).

-export([
    start_link/0,
    init/1
]).


start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).


init([]) ->
    Children = [
        {lethe_db_sup,
            {lethe_db_sup, start_link, []},
            permanent,
            100,
            supervisor,
            [lethe_db_sup]
        },
        {lethe_server,
            {lethe_server, start_link, []},
            permanent,
            100,
            worker,
            [lethe_server]
        }
    ],
    {ok, {{one_for_one, 5, 10}, Children}}.

