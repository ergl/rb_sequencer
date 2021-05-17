-module(rb_sequencer_dc_connection_manager).
-behavior(gen_server).
-include("rb_sequencer.hrl").
-include("dc_messages.hrl").
-include_lib("kernel/include/logger.hrl").

-define(REPLICAS_TABLE, connected_replicas).
-define(REPLICAS_TABLE_KEY, replicas).
-define(CONN_POOL_TABLE, connection_pools).

%% External API
-export([connect_to/1,
         connected_replicas/0]).

%% Red transactions
-export([send_red_prepare/7,
         send_red_accept_ack/5,
         send_red_already_decided/4,
         send_red_decision/5]).

%% Red heartbeats
-export([send_red_heartbeat/5,
         send_red_heartbeat_ack/4]).

%% Raw API
-export([send_raw_framed/2]).

%% Management API
-export([sender_pool_size/0,
         close/1]).

%% Used through erpc or supervisor machinery
-ignore_xref([start_link/0,
              connect_to/1,
              close/1]).

%% Supervisor
-export([start_link/0]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2]).

-record(state, {
    replicas :: cache(replicas, ordsets:ordset(replica_id())),
    connections :: cache({replica_id(), non_neg_integer()}, grb_dc_connection_sender_sup:handle())
}).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% DC API
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec connect_to(replica_descriptor()) -> ok | {error, term()}.
connect_to(#replica_descriptor{replica_id=ReplicaID, sequencer_ip=Ip, sequencer_port=Port}) ->
    ?LOG_DEBUG("Node connecting to DC ~p at ~p:~b", [ReplicaID, Ip, Port]),
    try
        {ok, Connections} = rb_sequencer_dc_connection_sender_sup:start_connection(ReplicaID, Ip, Port),
        ?LOG_DEBUG("DC connections: ~p", [Connections]),
        ok = add_replica_connections(ReplicaID, Connections),
        ok
    catch Exn -> Exn
    end.

%% @doc Close the connection to (all nodes at) the remote replica
-spec close(replica_id()) -> ok.
close(ReplicaId) ->
    gen_server:call(?MODULE, {close, ReplicaId}).

-spec sender_pool_size() -> non_neg_integer().
sender_pool_size() ->
    case persistent_term:get({?MODULE, sender_pool_size}, undefined) of
        undefined ->
            {ok, ConnNum} = application:get_env(rb_sequencer, inter_dc_pool_size),
            persistent_term:put({?MODULE, sender_pool_size}, ConnNum),
            ConnNum;
        Other ->
            Other
    end.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Node API
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec start_link() -> {ok, pid()} | ignore | {error, term()}.
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

-spec add_replica_connections(Id :: replica_id(),
                              Connections :: [grb_dc_connection_sender_sup:handle()]) -> ok.
add_replica_connections(Id, PartitionConnections) ->
    gen_server:call(?MODULE, {add_replica_connections, Id, PartitionConnections}).

-spec connected_replicas() -> [replica_id()].
connected_replicas() ->
    ets:lookup_element(?REPLICAS_TABLE, ?REPLICAS_TABLE_KEY, 2).

-spec send_red_prepare(ToId :: replica_id(),
                       Coordinator :: red_coord_location(),
                       TxId :: term(),
                       Label :: tx_label(),
                       PRS :: partition_rs(),
                       PWS :: partition_ws(),
                       VC :: vclock()) -> ok | {error, term()}.

send_red_prepare(ToId, Coordinator, TxId, Label, PRS, PWS, VC) ->
    send_raw_framed(
        ToId,
        rb_sequencer_dc_messages:frame(rb_sequencer_dc_messages:red_prepare(Coordinator, TxId, Label, PRS, PWS, VC))
    ).

-spec send_red_accept_ack(replica_id(), ballot(), term(), red_vote(), vclock()) -> ok.
send_red_accept_ack(ToId, Ballot, TxId, Vote, PrepareVC) ->
    send_raw_framed(
        ToId,
        rb_sequencer_dc_messages:frame(rb_sequencer_dc_messages:red_accept_ack(Ballot, Vote, TxId, PrepareVC))
    ).

-spec send_red_already_decided(replica_id(), term(), red_vote(), vclock()) -> ok.
send_red_already_decided(ToId, TxId, Decision, CommitVC) ->
    send_raw_framed(
        ToId,
        rb_sequencer_dc_messages:frame(rb_sequencer_dc_messages:red_already_decided(Decision, TxId, CommitVC))
    ).

-spec send_red_decision(replica_id(), ballot(), term(), red_vote(), grb_vclock:ts()) -> ok.
send_red_decision(ToId, Ballot, TxId, Decision, CommitTs) ->
    send_raw_framed(
        ToId,
        rb_sequencer_dc_messages:frame(rb_sequencer_dc_messages:red_decision(Ballot, Decision, TxId, CommitTs))
    ).

-spec send_red_heartbeat(ToId :: replica_id(),
                         Sequence :: non_neg_integer(),
                         Ballot :: ballot(),
                         Id :: term(),
                         Time :: grb_time:ts()) -> ok | {error, term()}.

send_red_heartbeat(ToId, Sequence, Ballot, Id, Time) ->
    send_raw_framed(
        ToId,
        rb_sequencer_dc_messages:frame(rb_sequencer_dc_messages:red_heartbeat(Sequence, Ballot, Id, Time))
    ).

-spec send_red_heartbeat_ack(replica_id(), ballot(), term(), grb_time:ts()) -> ok | {error, term()}.
send_red_heartbeat_ack(ToId, Ballot, Id, Time) ->
    send_raw_framed(
        ToId,
        rb_sequencer_dc_messages:frame(rb_sequencer_dc_messages:red_heartbeat_ack(Ballot, Id, Time))
    ).

-spec send_raw_framed(replica_id(), iolist()) -> ok | {error, term()}.
send_raw_framed(ToId, IOList) ->
    try
        Connection = ets:lookup_element(?CONN_POOL_TABLE, {ToId, rand:uniform(sender_pool_size())}, 2),
        rb_sequencer_dc_connection_sender:send_msg(Connection, IOList)
    catch _:_  ->
        {error, gone}
    end.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% gen_server callbacks
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

init([]) ->
    ReplicaTable = ets:new(?REPLICAS_TABLE, [set, protected, named_table, {read_concurrency, true}]),
    true = ets:insert(?REPLICAS_TABLE, {?REPLICAS_TABLE_KEY, ordsets:new()}),
    ConnPoolTable = ets:new(?CONN_POOL_TABLE, [ordered_set, protected, named_table, {read_concurrency, true}]),
    {ok, #state{replicas=ReplicaTable,
                connections=ConnPoolTable}}.

handle_call({add_replica_connections, ReplicaId, Connections}, _From, State) ->
    Replicas = connected_replicas(),
    true = ets:insert(?REPLICAS_TABLE, {?REPLICAS_TABLE_KEY, ordsets:add_element(ReplicaId, Replicas)}),
    ok = add_replica_connections_internal(ReplicaId, Connections),
    {reply, ok, State};

handle_call({close, ReplicaId}, _From, State) ->
    ?LOG_INFO("Closing connections to ~p", [ReplicaId]),
    Replicas = connected_replicas(),
    true = ets:insert(?REPLICAS_TABLE, {?REPLICAS_TABLE_KEY, ordsets:del_element(ReplicaId, Replicas)}),
    ok = close_replica_connections(ReplicaId, State),
    {reply, ok, State};

handle_call(E, _From, S) ->
    ?LOG_WARNING("~p unexpected call: ~p~n", [?MODULE, E]),
    {reply, ok, S}.

handle_cast(E, S) ->
    ?LOG_WARNING("~p unexpected cast: ~p~n", [?MODULE, E]),
    {noreply, S}.

handle_info(E, S) ->
    logger:warning("~p unexpected info: ~p~n", [?MODULE, E]),
    {noreply, S}.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% internal
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec add_replica_connections_internal(ReplicaId :: replica_id(),
                                       Connections :: [grb_dc_connection_sender_sup:handle()]) -> ok.

add_replica_connections_internal(ReplicaId, Connections) ->
    PoolSize = sender_pool_size(),
    Objects = [
        {{ReplicaId, Idx}, C}
        || {Idx, C} <- lists:zip(lists:seq(1, PoolSize), Connections)
    ],
    true = ets:insert(?CONN_POOL_TABLE, Objects),
    ok.

-spec close_replica_connections(replica_id(), #state{}) -> ok.
close_replica_connections(ReplicaId, #state{connections=Connections}) ->
    Handles = ets:select(Connections, [{ {{ReplicaId, '$1'}, '$2'}, [], [{{'$1', '$2'}}] }]),
    lists:foreach(
        fun({Idx, Handle}) ->
            true = ets:delete(Connections, {ReplicaId, Idx}),
            ok = rb_sequencer_dc_connection_sender:close(Handle)
        end,
        Handles
    ).

