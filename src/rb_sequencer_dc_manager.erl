-module(rb_sequencer_dc_manager).
-behavior(gen_server).
-include("rb_sequencer.hrl").
-include("dc_messages.hrl").
-include_lib("kernel/include/logger.hrl").

-define(MY_REPLICA, my_replica).
-define(ALL_REPLICAS, all_replicas).
-define(REMOTE_REPLICAS, remote_replicas).

-define(PARTITION, partition_key).
-define(IS_DC_STARTED, is_dc_started).

%% Node API
-export([replica_id/0,
         all_replicas/0,
         remote_replicas/0]).

-export([connection_for/1,
         connected_nodes/0,
         partition_node/1,
         save_partition_information/3]).

%% Remote API
-export([create_replica_groups/1,
         replica_descriptor/0,
         make_replica_descriptor/3,
         connect_to_replicas/1,
         start_paxos_leader/0,
         start_paxos_follower/0]).

%% Supervisor
-export([start_link/0]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2]).

%% All functions here are called through erpc
-ignore_xref([start_link/0,
              create_replica_groups/1,
              single_replica_processes/0,
              replica_descriptor/0,
              connect_to_replicas/1,
              start_paxos_leader/0,
              start_paxos_follower/0]).

-define(TABLE, rb_sequencer_dc_manager_state_table).
-define(LOCAL_GRB_CONN_POOL_SIZE, local_grb_conn_pool_size).
-record(state, {
    seen_for_node = #{} :: #{inet:ip_address() => non_neg_integer()},
    state_table :: cache(#{
        %% mapping of connection pools
        {inet:ip_address(), non_neg_integer()} := rb_sequencer_tcp_server:t(),

        %% list of all connected nodes, useful for broadcast
        grb_nodes := #{inet:ip_address() := []},

        %% Inverse mapping from partition to owner node
        partition_id() => inet:ip_address()
    })
}).

-type target_node_list() :: [{replica_id(), inet:ip_address(), inet:port_number()}].
-type replica_groups_state() :: {replica_id(), [replica_id()], [{replica_id(), inet:socket()}]}.

-spec is_joined() -> boolean().
is_joined() ->
    persistent_term:get({?MODULE, ?IS_DC_STARTED}, false).

-spec set_joined() -> ok.
set_joined() ->
    persistent_term:put({?MODULE, ?IS_DC_STARTED}, true).

-spec toggle_joined() -> ok | {error, already_joined}.
toggle_joined() ->
    case is_joined() of
        false ->
            set_joined(),
            ok;
        true ->
            {error, already_joined}
    end.

-spec replica_id() -> replica_id().
replica_id() ->
    persistent_term:get({?MODULE, ?MY_REPLICA}).

-spec set_replica_id(replica_id()) -> ok.
set_replica_id(ReplicaName) ->
    persistent_term:put({?MODULE, ?MY_REPLICA}, ReplicaName).

-spec all_replicas() -> [replica_id()].
all_replicas() ->
    persistent_term:get({?MODULE, ?ALL_REPLICAS}, [replica_id()]).

-spec set_all_replicas([replica_id()]) -> ok.
set_all_replicas(AllReplicas) ->
    persistent_term:put({?MODULE, ?ALL_REPLICAS}, AllReplicas).

-spec remote_replicas() -> [replica_id()].
remote_replicas() ->
    persistent_term:get({?MODULE, ?REMOTE_REPLICAS}, []).

-spec cache_remote_replicas() -> ok.
cache_remote_replicas() ->
    MyReplica = replica_id(),
    AllReplicas = all_replicas(),
    persistent_term:put({?MODULE, ?REMOTE_REPLICAS}, AllReplicas -- [MyReplica]).

-spec create_replica_groups(target_node_list()) -> {ok, [replica_id()]} | {error, term()}.
create_replica_groups(Args) ->
    case toggle_joined() of
        {error, Reason} ->
            {error, Reason};
        ok ->
            create_replica_groups_(Args)
    end.

-spec create_replica_groups_(target_node_list()) -> {ok, [replica_id()]} | {error, term()}.
create_replica_groups_([{RegionName, _, _}]) ->
    ok = set_replica_id(RegionName),
    ok = set_all_replicas([RegionName]),
    ok = rb_sequencer_service_manager:set_leader(RegionName),
    ok = rb_sequencer_service_manager:cache_quorum_size(),
    ok = start_paxos_leader(),
    {ok, [RegionName]};

create_replica_groups_(Nodes) ->
    ?LOG_INFO("Starting connecting all replicas: ~p~n", [Nodes]),
    State = create_replica_groups_state(Nodes),
    Res = case create_replica_groups_descriptors(State) of
        {error, Reason} ->
            ?LOG_ERROR("replica_descriptor error: ~p~n", [Reason]),
            {error, Reason};

        {ok, Descriptors} ->
            case create_replica_groups_send_descriptors(Descriptors, State) of
                {error, Reason} ->
                    ?LOG_ERROR("connect_to_replica error: ~p~n", [Reason]),
                    {error, Reason};

                ok ->
                     case start_paxos_processes(State) of
                        {error, Reason} ->
                            {error, Reason};

                        ok ->
                            Ids = [Id || #replica_descriptor{replica_id=Id} <- Descriptors],
                            {ok, Ids}
                    end
            end
    end,
    ok = terminate_replica_groups_state(State),
    Res.

-spec create_replica_groups_state(target_node_list()) -> replica_groups_state().
create_replica_groups_state(Targets) ->
    %% Mark ourselves as the leader DC
    {MyIP, MyPort} = rb_sequencer_dc_utils:inter_dc_ip_port(),
    lists:foldl(fun({Region, IPStr, Port}, {LeaderRegion, AllReplicas, Acc}) ->
        {ok, IP} = inet:parse_address(IPStr),
        if
            (IP =:= MyIP) and (Port =:= MyPort) ->
                %% Skip ourselves
                %% Overwrite leader region, it's alway sus
                {Region, [Region | AllReplicas], Acc};
            true ->
                SockOpts = lists:keyreplace(active, 1, ?INTER_DC_SOCK_OPTS, {active, false}),
                {ok, Socket} = gen_tcp:connect(IP, Port, SockOpts),
                {LeaderRegion, [Region | AllReplicas], [ {Region, Socket} | Acc]}
        end
    end, {undefined, [], []}, Targets).

-spec terminate_replica_groups_state(replica_groups_state()) -> ok.
terminate_replica_groups_state({_, _, SocketList}) ->
    [ gen_tcp:close(S) || {_, S} <- SocketList ],
    ok.

-spec create_replica_groups_descriptors(replica_groups_state()) -> {ok, [replica_descriptor()]} | {error, atom()}.
create_replica_groups_descriptors({LeaderRegion, AllRegions, Sockets}) ->
    %% This is always called at the leader
    ok = make_replica_descriptor(LeaderRegion, LeaderRegion, AllRegions),
    lists:foldl(fun
        (_, {error, Reason}) ->
            {error, Reason};

        ({SocketReplica, Socket}, {ok, Acc}) ->
            %% Send the info to all
            Payload = term_to_binary({LeaderRegion, AllRegions, SocketReplica}),
            ok = gen_tcp:send(Socket, <<?VERSION:?VERSION_BITS, ?DC_GET_DESCRIPTOR:?MSG_KIND_BITS, Payload/binary>>),
            case gen_tcp:recv(Socket, 0) of
                {error, Reason} ->
                    {error, Reason};
                {ok, Bin} ->
                    {ok, [binary_to_term(Bin) | Acc]}
        end
    end, {ok, [replica_descriptor()]}, Sockets).

-spec create_replica_groups_send_descriptors([replica_descriptor()], replica_groups_state()) -> ok | {error, atom()}.
create_replica_groups_send_descriptors(Descriptors, {_, _, Sockets}) ->
    DescrBin = term_to_binary(Descriptors),
    lists:foldl(fun
        (_, {error, Reason}) ->
            {error, Reason};
        ({_, Socket}, ok) ->
            Msg = <<?VERSION:?VERSION_BITS, ?DC_CONNECT_TO_DESCR:?MSG_KIND_BITS, DescrBin/binary>>,
            ok = gen_tcp:send(Socket, Msg),
            case gen_tcp:recv(Socket, 0) of
                {error, Reason} ->
                    {error, Reason};
                {ok, Bin} ->
                    binary_to_term(Bin)
        end
    end, connect_to_replicas(Descriptors), Sockets).

-spec start_paxos_processes(replica_groups_state()) -> ok | {error, term()}.
start_paxos_processes({LeaderReplica, _, Sockets}) ->
    Msg = <<?VERSION:?VERSION_BITS, ?DC_START_PAXOS_FOLLOWER:?MSG_KIND_BITS>>,
    Res = lists:foldl(
        fun
            (_, {error, Reason}) ->
                {error, Reason};
            ({_, Socket}, ok) ->
                ok = gen_tcp:send(Socket, Msg),
                case gen_tcp:recv(Socket, 0) of
                    {error, Reason} ->
                        {error, Reason};
                    {ok, <<>>} ->
                        ok
                end
        end,
        ok,
        Sockets
    ),
    case Res of
        {error, Reason} ->
            {error, Reason};
        ok ->
            ?LOG_INFO("started red processes, leader cluster: ~p", [LeaderReplica]),
            ok = start_paxos_leader()
    end.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% External API
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec start_paxos_leader() -> ok.
start_paxos_leader() ->
    ok = rb_sequencer_service:init_leader_state(),
    ok = rb_sequencer_service_manager:start_coordinators(),
    ok.

-spec start_paxos_follower() -> ok.
start_paxos_follower() ->
    ok = rb_sequencer_service:init_follower_state(),
    ok = rb_sequencer_service_manager:start_coordinators(),
    ok.

-spec make_replica_descriptor(replica_id(), replica_id(), [replica_id()]) -> ok.
make_replica_descriptor(LeaderRegion, MyRegion, AllRegions) ->
    ok = set_replica_id(MyRegion),
    ok = set_all_replicas(AllRegions),
    ok = cache_remote_replicas(),
    ok = rb_sequencer_service_manager:set_leader(LeaderRegion),
    ok = rb_sequencer_service_manager:cache_quorum_size().

%% @doc Get the descriptor for this replica/cluster.
%%
%%      Contains information from all the nodes in the cluster
%%      so it is enough to call this function at a single node
%%      in an entire DC.
%%
-spec replica_descriptor() -> replica_descriptor().
replica_descriptor() ->
    {ok, IP} = application:get_env(rb_sequencer, inter_dc_ip),
    {ok, Port} = application:get_env(rb_sequencer, inter_dc_port),
    #replica_descriptor{
        replica_id=replica_id(),
        sequencer_ip=IP,
        sequencer_port=Port
    }.

%% @doc Commands this sequencer to join to all given descriptors
-spec connect_to_replicas([replica_descriptor()]) -> ok | {error, term()}.
connect_to_replicas(Descriptors) ->
    connect_to_replicas(Descriptors, replica_id()).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Internal Functions
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec connect_to_replicas([replica_descriptor()], replica_id()) -> ok | {error, term()}.
connect_to_replicas([], _) -> ok;
connect_to_replicas([#replica_descriptor{replica_id=Id} | Rest], Id) ->
    %% Skip myself
    connect_to_replicas(Rest, Id);
connect_to_replicas([Desc | Rest], LocalId) ->
    #replica_descriptor{replica_id=RemoteId} = Desc,
    ?LOG_DEBUG("Starting join DC ~p", [RemoteId]),
    case rb_sequencer_dc_connection_manager:connect_to(Desc) of
        {error, Reason} ->
            ?LOG_ERROR("Node errored with ~p while connecting to DC ~p", [Reason, RemoteId]),
            {error, {bad_remote_connect, Reason}};
        ok ->
            connect_to_replicas(Rest, LocalId)
    end.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% gen_server API and callbacks
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec start_link() -> {ok, pid()} | ignore | {error, term()}.
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

-spec partition_node(partition_id()) -> inet:ip_address().
partition_node(Partition) ->
    ets:lookup_element(?TABLE, Partition, 2).

-spec save_partition_information(rb_sequencer_tcp_server:t(), inet:ip_address(), [partition_id()]) -> ok.
save_partition_information(ConnectionPid, RemoteIp, Partitions) ->
    gen_server:cast(?MODULE, {add_partition_info, RemoteIp, ConnectionPid, Partitions}).

-spec connected_nodes() -> [inet:ip_address()].
connected_nodes() ->
    maps:keys(ets:lookup_element(?TABLE, grb_nodes, 2)).

-spec connection_for(inet:ip_address()) -> rb_sequencer_tcp_server:t().
connection_for(Node) ->
    ets:lookup_element(
        ?TABLE,
        {Node, rand:uniform(local_connection_pool_size())},
        2
    ).

-spec local_connection_pool_size() -> non_neg_integer().
local_connection_pool_size() ->
    persistent_term:get({?MODULE, ?LOCAL_GRB_CONN_POOL_SIZE}).

init(_) ->
    {ok, LocalConnPoolSize} = application:get_env(rb_sequencer, local_grb_node_connection_pool),
    persistent_term:put({?MODULE, ?LOCAL_GRB_CONN_POOL_SIZE}, LocalConnPoolSize),
    ?TABLE = ets:new(?TABLE, [set, named_table, {read_concurrency, true}]),
    true = ets:insert(?TABLE, {grb_nodes, #{}}),
    {ok, #state{state_table=?TABLE}}.

handle_call(E, _From, S) ->
    ?LOG_WARNING("~p unexpected call: ~p~n", [?MODULE, E]),
    {reply, ok, S}.

handle_cast({add_partition_info, RemoteIp, ConnPid, Partitions}, S=#state{seen_for_node=NextIdxMap, state_table=Table}) ->
    Idx = maps:get(RemoteIp, NextIdxMap, 1),
    true = ets:insert(Table, {{RemoteIp, Idx}, ConnPid}),
    AllNodes = ets:lookup_element(Table, grb_nodes, 2),
    if
        is_map_key(RemoteIp, AllNodes) ->
            %% We've already registered this node, do nothing
            ok;
        true ->
            %% Register this node and all its partitions
            Objects = [ {P, RemoteIp} || P <- Partitions ],
            true = ets:insert(Table, Objects),
            true = ets:insert(Table, {grb_nodes, AllNodes#{RemoteIp => []}})
    end,
    {noreply, S#state{seen_for_node=NextIdxMap#{RemoteIp => Idx + 1}}};

handle_cast(E, S) ->
    ?LOG_WARNING("~p unexpected cast: ~p~n", [?MODULE, E]),
    {noreply, S}.

handle_info(E, S) ->
    logger:warning("~p unexpected info: ~p~n", [?MODULE, E]),
    {noreply, S}.
