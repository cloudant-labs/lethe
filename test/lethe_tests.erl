-module(lethe_tests).


-include_lib("eunit/include/eunit.hrl").


lethe_test_()->
    test_engine_util:create_tests(lethe).

