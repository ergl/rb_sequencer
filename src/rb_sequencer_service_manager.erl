-module(rb_sequencer_service_manager).
-behavior(gen_server).
-include("rb_sequencer.hrl").
-include_lib("kernel/include/logger.hrl").

%% Supervisor
-export([start_link/0]).
%% erpc
-ignore_xref([start_link/0,
              start_coordinators/0]).

-export([leader/0,
         set_leader/1,
         quorum_size/0,
         cache_quorum_size/0,
         start_coordinators/0,
         register_coordinator/1,
         transaction_coordinator/1,
         unregister_coordinator/2]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2]).

-define(COORD_TABLE, rb_sequencer_service_manager_coordinators).
-define(QUORUM_KEY, quorum_size).
-define(POOL_SIZE, pool_size).
-define(COORD_KEY, coord_key).
-define(LEADER_KEY, leader_key).

-record(state, { pid_for_tx :: cache(term(), red_coordinator()) }).

-spec start_link() -> {ok, pid()} | ignore | {error, term()}.
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

-spec calc_quorum_size() -> non_neg_integer().
calc_quorum_size() ->
    {ok, FaultToleranceFactor} = application:get_env(rb_sequencer, fault_tolerance_factor),
    Factor = (FaultToleranceFactor + 1),
    %% peg between 1 and the size of all replicas to avoid bad usage
    max(1, min(Factor, length(rb_sequencer_dc_manager:all_replicas()))).

-spec cache_quorum_size() -> ok.
cache_quorum_size() ->
    persistent_term:put({?MODULE, ?QUORUM_KEY}, calc_quorum_size()).

-spec quorum_size() -> non_neg_integer().
quorum_size() ->
    persistent_term:get({?MODULE, ?QUORUM_KEY}).

-spec leader() -> leader_location().
leader() ->
    persistent_term:get({?MODULE, ?LEADER_KEY}).

-spec set_leader(replica_id()) -> ok.
set_leader(LeaderReplica) ->
    persistent_term:put({?MODULE, ?LEADER_KEY}, LeaderReplica).

-spec start_coordinators() -> ok.
start_coordinators() ->
    {ok, PoolSize} = application:get_env(rb_sequencer, red_coord_pool_size),
    ok = persistent_term:put({?MODULE, ?POOL_SIZE}, PoolSize),
    start_coordinators(PoolSize).

-spec start_coordinators(non_neg_integer()) -> ok.
start_coordinators(0) ->
    ok;
start_coordinators(N) ->
    {ok, Pid} = rb_sequencer_coordinator_sup:start_coordinator(N),
    ok = persistent_term:put({?MODULE, ?COORD_KEY, N}, Pid),
    start_coordinators(N - 1).

-spec register_coordinator(term()) -> red_coordinator().
register_coordinator(TxId) ->
    PoolSize = persistent_term:get({?MODULE, ?POOL_SIZE}),
    WorkerPid = persistent_term:get({?MODULE, ?COORD_KEY, rand:uniform(PoolSize)}),
    true = ets:insert(?COORD_TABLE, {TxId, WorkerPid}),
    WorkerPid.

-spec transaction_coordinator(term()) -> {ok, red_coordinator()} | error.
transaction_coordinator(TxId) ->
    case ets:lookup(?COORD_TABLE, TxId) of
        [{TxId, Pid}] -> {ok, Pid};
        _ -> error
    end.

-spec unregister_coordinator(term(), pid()) -> ok.
unregister_coordinator(TxId, Pid) ->
    true = ets:delete_object(?COORD_TABLE, {TxId, Pid}),
    ok.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% gen_server callbacks
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

init([]) ->
    CoordTable = ets:new(?COORD_TABLE, [set, public, named_table,
                                        {read_concurrency, true}, {write_concurrency, true}]),
    {ok, #state{pid_for_tx=CoordTable}}.

handle_call(E, _From, S) ->
    ?LOG_WARNING("~p unexpected call: ~p~n", [?MODULE, E]),
    {reply, ok, S}.

handle_cast(E, S) ->
    ?LOG_WARNING("~p unexpected cast: ~p~n", [?MODULE, E]),
    {noreply, S}.

handle_info(E, S) ->
    ?LOG_WARNING("~p unexpected info: ~p~n", [?MODULE, E]),
    {noreply, S}.
