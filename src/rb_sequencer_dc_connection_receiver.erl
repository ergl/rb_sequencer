-module(rb_sequencer_dc_connection_receiver).

-behaviour(gen_server).
-behavior(ranch_protocol).
-include("rb_sequencer.hrl").
-include("dc_messages.hrl").
-include_lib("kernel/include/logger.hrl").

%% Module API
-export([start_service/0]).

%% ranch_protocol callback
-export([start_link/4]).

%% API
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2]).

-define(SERVICE, rb_sequencer_inter_dc).
-define(SERVICE_POOL, (1 * erlang:system_info(schedulers_online))).

-define(EXPAND_BUFFER_INTERVAL, 100).

-record(state, {
    socket :: inet:socket(),
    transport :: module(),
    sender_replica = undefined :: replica_id() | undefined,
    recalc_buffer_timer = undefined :: reference() | undefined
}).

-spec start_service() -> ok.
start_service() ->
    start_service(?SERVICE, application:get_env(rb_sequencer, inter_dc_port)).

start_service(ServiceName, {ok, Port}) ->
    {ok, _} = ranch:start_listener(ServiceName, ranch_tcp,
                                   [{port, Port}, {num_acceptors, ?SERVICE_POOL}, {max_connections, infinity}],
                                   ?MODULE, []),
    ActualPort = ranch:get_port(ServiceName),
    ?LOG_INFO("~p server started on port ~p", [?MODULE, ActualPort]),
    ok.

%% Ranch workaround for gen_server
%% Socket is deprecated, will be removed
start_link(Ref, _Sock, Transport, Opts) ->
    {ok, proc_lib:spawn_link(?MODULE, init, [{Ref, Transport, Opts}])}.

init({Ref, Transport, _Opts}) ->
    {ok, Socket} = ranch:handshake(Ref),
    ok = ranch:remove_connection(Ref),
    ok = Transport:setopts(Socket, ?INTER_DC_SOCK_OPTS),
    State = #state{socket=Socket, transport=Transport},
    gen_server:enter_loop(?MODULE, [], State).

handle_call(E, From, S) ->
    ?LOG_WARNING("~p got unexpected call with msg ~w from ~w", [?MODULE, E, From]),
    {reply, ok, S}.

handle_cast(E, S) ->
    ?LOG_WARNING("~p got unexpected cast with msg ~w", [?MODULE, E]),
    {noreply, S}.

terminate(_Reason, #state{socket=Socket, transport=Transport}) ->
    catch Transport:close(Socket),
    ok.

handle_info(
    {tcp, Socket, <<?VERSION:?VERSION_BITS, ?DC_CREATE:?MSG_KIND_BITS,
                    NodesBin/binary>>},
    State = #state{socket=Socket, transport=Transport}
) ->
    Nodes = binary_to_term(NodesBin),
    Resp = rb_sequencer_dc_manager:create_replica_groups(Nodes),
    Transport:send(Socket, term_to_binary(Resp)),
    Transport:setopts(Socket, [{active, once}]),
    {noreply, State};

handle_info(
    {tcp, Socket, <<?VERSION:?VERSION_BITS, ?DC_GET_DESCRIPTOR:?MSG_KIND_BITS,
                    Payload/binary>>},
    State = #state{socket=Socket, transport=Transport}
) ->
    {LeaderRegion, AllRegions, MyRegion} = binary_to_term(Payload),
    ok = rb_sequencer_dc_manager:make_replica_descriptor(LeaderRegion, MyRegion, AllRegions),
    Transport:send(Socket, term_to_binary(rb_sequencer_dc_manager:replica_descriptor())),
    Transport:setopts(Socket, [{active, once}]),
    {noreply, State};

handle_info(
    {tcp, Socket, <<?VERSION:?VERSION_BITS, ?DC_CONNECT_TO_DESCR:?MSG_KIND_BITS,
                    Payload/binary>>},
    State = #state{socket=Socket, transport=Transport}
) ->
    Descriptors = binary_to_term(Payload),
    Res = rb_sequencer_dc_manager:connect_to_replicas(Descriptors),
    Transport:send(Socket, term_to_binary(Res)),
    Transport:setopts(Socket, [{active, once}]),
    {noreply, State};

handle_info(
    {tcp, Socket, <<?VERSION:?VERSION_BITS, ?DC_START_PAXOS_FOLLOWER:?MSG_KIND_BITS>>},
    State = #state{socket=Socket, transport=Transport}
) ->
    ok = rb_sequencer_dc_manager:start_paxos_follower(),
    Transport:send(Socket, <<>>),
    Transport:setopts(Socket, [{active, once}]),
    {noreply, State};

handle_info(
    {tcp, Socket, <<?VERSION:?VERSION_BITS, ?DC_PING:?MSG_KIND_BITS,
                    Payload/binary>>},
    State = #state{socket=Socket, transport=Transport}
) ->
    SenderReplica = binary_to_term(Payload),
    ?LOG_DEBUG("Received connect ping from ~p", [SenderReplica]),
    Transport:setopts(Socket, [{active, once}]),
    ok = expand_drv_buffer(Transport, Socket),
    {noreply, State#state{sender_replica=SenderReplica,
                          recalc_buffer_timer=erlang:send_after(?EXPAND_BUFFER_INTERVAL, self(), recalc_buffer)}};

handle_info(
    {tcp, Socket, <<?VERSION:?VERSION_BITS, Payload/binary>>},
    State = #state{socket=Socket,
                   transport=Transport,
                   sender_replica=SenderReplica}
) ->
    Request = rb_sequencer_dc_messages:decode_payload(Payload),
    ?LOG_DEBUG("Received msg to from ~p: ~p", [SenderReplica, Request]),
    ok = handle_request(SenderReplica, Request),
    Transport:setopts(Socket, [{active, once}]),
    {noreply, State};

handle_info({tcp, Socket, Data}, State = #state{transport=Transport}) ->
    ?LOG_WARNING("~p received unknown data ~p", [?MODULE, Data]),
    Transport:setopts(Socket, [{active, once}]),
    {noreply, State};

handle_info({tcp_closed, _Socket}, S) ->
    ?LOG_INFO("inter_dc_connection received tcp_closed"),
    {stop, normal, S};

handle_info({tcp_error, _Socket, Reason}, S) ->
    ?LOG_INFO("inter_dc_connection received tcp_error ~p", [Reason]),
    {stop, Reason, S};

handle_info(timeout, State) ->
    ?LOG_INFO("inter_dc_connection received timeout"),
    {stop, normal, State};

handle_info(recalc_buffer, State = #state{socket=Socket,
                                          transport=Transport,
                                          recalc_buffer_timer=Ref}) ->
    ?CANCEL_TIMER_FAST(Ref),
    ok = expand_drv_buffer(Transport, Socket),
    {noreply, State#state{recalc_buffer_timer=erlang:send_after(?EXPAND_BUFFER_INTERVAL, self(), recalc_buffer)}};

handle_info(E, S) ->
    ?LOG_WARNING("inter_dc_connection received unexpected info with msg ~w", [E]),
    {noreply, S}.

%% Expand driver buffer size from time to time
%% This will eventually settle in a stable state if the recbuf stops changing.
-spec expand_drv_buffer(module(), gen_tcp:socket()) -> ok.
expand_drv_buffer(Transport, Socket) ->
    case Transport:getopts(Socket, [recbuf, buffer]) of
        {error, _} ->
            %% socket might have closed, nothing we can do
            ok;

        {ok, Proplist} ->
            {recbuf, RecBuffer} = lists:keyfind(recbuf, 1, Proplist),
            {buffer, DrvBuffer0} = lists:keyfind(buffer, 1, Proplist),
            DrvBuffer = erlang:max(RecBuffer * 2, DrvBuffer0),
            case Transport:setopts(Socket, [{buffer, DrvBuffer}]) of
                {error, _} ->
                    %% No room to expand, keep it the same
                    %% (if there's an error again, the socket might have been closed)
                    Transport:setopts(Socket, [{buffer, DrvBuffer0}]);
                ok ->
                    ok
            end
    end.

-spec handle_request(replica_id(), replica_message()) -> ok.
handle_request(_, #red_prepare{
    coord_location=Coord,
    tx_id=TxId,
    tx_label=Label,
    readset=PRS,
    writeset=PWS,
    snapshot_vc=VC
}) ->
    rb_sequencer_service:prepare(TxId, Label, PRS, PWS, VC, Coord);

handle_request(_, #red_accept{
    coord_location = Coord,
    ballot = Ballot,
    tx_id = TxId,
    tx_label = Label,
    readset = PRS,
    writeset = PWS,
    decision = Vote,
    prepare_vc = PrepareVC,
    sequence_number = SequenceNumber
}) ->
    rb_sequencer_service:accept(SequenceNumber, Ballot, TxId, Label, PRS, PWS, Vote, PrepareVC, Coord);

handle_request(_, #red_accept_ack{
    ballot = Ballot,
    tx_id = TxId,
    decision = Vote,
    prepare_vc = PrepareVC
}) ->
    rb_sequencer_coordinator:accept_ack(Ballot, TxId, Vote, PrepareVC);

handle_request(_, #red_decision{
    ballot = Ballot,
    tx_id = TxId,
    decision = Decision,
    commit_ts = CommitTs
}) ->
    rb_sequencer_service:decide(Ballot, TxId, Decision, CommitTs);

handle_request(_, #red_learn_abort{
    ballot = Ballot,
    tx_id = TxId,
    reason = Reason,
    commit_ts = CommitTs
}) ->
    rb_sequencer_service:learn_abort(Ballot, TxId, Reason, CommitTs);

handle_request(_, #red_already_decided{
    tx_id = TxId,
    decision = Decision,
    commit_vc = CommitVC
}) ->
    rb_sequencer_coordinator:already_decided(TxId, Decision, CommitVC);

handle_request(_, #red_deliver{
    ballot = Ballot,
    sequence_number = SequenceNumber,
    timestamp = Ts,
    transactions = Transactions
}) ->
    rb_sequencer_service:deliver(SequenceNumber, Ballot, Ts, Transactions);

handle_request(FromReplica, #red_heartbeat{
    ballot = Ballot,
    heartbeat_id = HeartbeatId,
    timestamp = Ts,
    sequence_number = SequenceNumber
}) ->
    rb_sequencer_service:accept_heartbeat(FromReplica, SequenceNumber, Ballot, HeartbeatId, Ts);

handle_request(_, #red_heartbeat_ack{
    ballot = Ballot,
    heartbeat_id = HeartbeatId,
    timestamp = Ts
}) ->
    rb_sequencer_heartbeat:handle_accept_ack(Ballot, HeartbeatId, Ts).
