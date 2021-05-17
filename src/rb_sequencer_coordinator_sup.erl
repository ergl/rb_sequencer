-module(rb_sequencer_coordinator_sup).
-behavior(supervisor).
-include("rb_sequencer.hrl").

-export([start_link/0,
         start_coordinator/1]).

%% API
-export([init/1]).

-ignore_xref([start_link/0]).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

start_coordinator(Id) ->
    supervisor:start_child(?MODULE, [Id]).

init([]) ->
    {ok, {{simple_one_for_one, 5, 10},
        [{rb_sequencer_coordinator,
            {rb_sequencer_coordinator, start_link, []},
            transient, 5000, worker, [rb_sequencer_coordinator]}]
    }}.
