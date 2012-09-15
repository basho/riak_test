-module(giddyup).

-export([get_suite/3]).

-define(SUITE_PATH, "http://~s/project/~s?platform=~s").

get_suite(Host, Project, Platform) ->
    Schema = get_schema(Host, Project, Platform),
    Proj = kvc:path(project, Schema),
    Name = kvc:path(name, Proj),
    lager:info("Retrieved Project: ~s", [Name]),
    Tests = kvc:path(tests, Proj),
    [ {
        kvc:path(id, Test),
        binary_to_atom(kvc:path(name, Test), utf8),
        case kvc:path(tags.backend, Test) of [] -> na; X -> binary_to_atom(X, utf8) end
      } || Test <- Tests].
    
get_schema(Host, Project, Platform) ->
    URL = io_lib:format(?SUITE_PATH, [Host, Project, Platform]),
    lager:info("giddyup url: ~s", [URL]),
    JSON = <<"{
              \"project\":{
                \"name\":\"riak\",
                \"tests\":[
                  {\"id\":1, \"name\":\"verify_build_cluster\",
                   \"tags\":{\"platform\":\"ubuntu-1004-64\"}},
                  {\"id\":2, \"name\":\"secondary_index_tests\",
                  \"tags\":{\"backend\":\"eleveldb\", \"platform\":\"ubuntu-1004-64\"}}
                ]
              }
            }">>,
    mochijson2:decode(JSON).
    