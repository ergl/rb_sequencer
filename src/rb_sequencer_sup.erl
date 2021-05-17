-module(rb_sequencer_sup).

-behaviour(supervisor).

%% API
-export([start_link/0]).

%% Supervisor callbacks
-export([init/1]).

%% Helper macro for declaring children of supervisor
-define(CHILD(I, Type, Args),
    {I, {I, start_link, Args}, permanent, 5000, Type, [I]}).

%% ===================================================================
%% API functions
%% ===================================================================

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

%% ===================================================================
%% Supervisor callbacks
%% ===================================================================

init(_Args) ->
    ChildSpecs = [
        ?CHILD(rb_sequencer_dc_manager, worker, []),
        ?CHILD(rb_sequencer_dc_connection_sender_sup, supervisor, []),
        ?CHILD(rb_sequencer_dc_connection_manager, worker, []),
        ?CHILD(rb_sequencer_coordinator_sup, supervisor, []),
        ?CHILD(rb_sequencer_service_manager, worker, []),
        ?CHILD(rb_sequencer_service, worker, [])
    ],

    {ok, {{one_for_one, 5, 10}, ChildSpecs}}.
