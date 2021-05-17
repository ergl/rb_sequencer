-module(rb_sequencer_time).

%% API
-type ts() :: non_neg_integer().
-export_type([ts/0]).
-export([timestamp/0]).

-spec timestamp() -> ts().
timestamp() ->
    erlang:system_time(micro_seconds).
