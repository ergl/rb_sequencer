-module(rb_sequencer_tcp_server).
-behaviour(gen_server).
-behavior(ranch_protocol).
-include("rb_sequencer.hrl").
-include_lib("kernel/include/logger.hrl").
-include_lib("pvc_types/include/sequencer_messages.hrl").

%% Module API
-export([start_server/0]).

%% ranch_protocol callback
-export([start_link/4]).

-export([send_msg/2,
         deliver_heartbeat/2,
         send_commit_response/3,
         send_delivery/3]).

%% API
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2]).

-define(TCP_NAME, tcp_server).
-define(TCP_PORT, 7878).
-define(TCP_ACC_POOL, (1 * erlang:system_info(schedulers_online))).

-type t() :: pid().
-export_type([t/0]).

-record(state, {
    socket :: inet:socket(),
    transport :: module(),
    remote_ip :: inet:ip_address(),
    pending_buffer = <<>> :: binary()
}).

start_server() ->
    DefaultPort = application:get_env(rb_sequencer, tcp_port, ?TCP_PORT),
    {ok, _}  = ranch:start_listener(?TCP_NAME,
                                    ranch_tcp,
                                    [{port, DefaultPort},
                                     {num_acceptors, ?TCP_ACC_POOL},
                                     {max_connections, infinity}],
                                    rb_sequencer_tcp_server,
                                    []),
    Port = ranch:get_port(?TCP_NAME),
    ?LOG_INFO("~p server started on port ~p", [?MODULE, Port]),
    ok.

%% Ranch workaround for gen_server
%% Socket is deprecated, will be removed
start_link(Ref, _Sock, Transport, Opts) ->
    {ok, proc_lib:spawn_link(?MODULE, init, [{Ref, Transport, Opts}])}.

-spec send_commit_response(t(), term(), {ok, vclock()} | {abort, atom()}) -> ok.
send_commit_response(Connection, TxId, Outcome) ->
    send_msg_pid(
        Connection,
        frame(sequencer_messages:prepare_response(TxId, Outcome))
    ).

-spec deliver_heartbeat(inet:ip_address(), rb_sequencer_time:ts()) -> ok.
deliver_heartbeat(NodeIp, Timestamp) ->
    send_msg_pid(
        rb_sequencer_dc_manager:connection_for(NodeIp),
        frame(sequencer_messages:deliver(Timestamp, []))
    ).

-spec send_delivery(inet:ip_address(), rb_sequencer_time:ts(), [{term(), partition_ws(), vclock()}]) -> ok.
send_delivery(NodeIp, Timestamp, TxList) ->
    send_msg_pid(
      rb_sequencer_dc_manager:connection_for(NodeIp),
        frame(sequencer_messages:deliver(Timestamp, TxList))
    ).

-spec send_msg(inet:ip_address(), iodata()) -> ok.
send_msg(NodeIp, Msg) ->
    send_msg_pid(
        rb_sequencer_dc_manager:connection_for(NodeIp),
        Msg
    ).

%% Note: This should already be framed
-spec send_msg_pid(t(), iodata()) -> ok.
send_msg_pid(Pid, Msg) ->
    gen_server:cast(Pid, {send, Msg}).

-spec frame(iodata()) -> iodata().
frame(Msg) -> sequencer_messages:frame(4, Msg).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init({Ref, Transport, _Opts}) ->
    {ok, Socket} = ranch:handshake(Ref),
    ok = ranch:remove_connection(Ref),
    ok = Transport:setopts(Socket, [{active, once}, {packet, raw}, {nodelay, true}]),
    {ok, {RemoteIp, _}} = inet:peername(Socket),
    State = #state{socket=Socket, transport=Transport, remote_ip=RemoteIp},
    gen_server:enter_loop(?MODULE, [], State).

handle_call(E, From, S) ->
    ?LOG_WARNING("~p server got unexpected call with msg ~w from ~w", [?MODULE, E, From]),
    {reply, ok, S}.

handle_cast({send, Msg}, S=#state{socket=Socket, transport=Transport}) ->
    Transport:send(Socket, Msg),
    {noreply, S};

handle_cast(E, S) ->
    ?LOG_WARNING("~p server got unexpected cast with msg ~w", [?MODULE, E]),
    {noreply, S}.

terminate(_Reason, #state{socket=Socket, transport=Transport}) ->
    catch Transport:close(Socket),
    ok.

handle_info({tcp, Socket, Data}, State = #state{socket=Socket,
                                                pending_buffer=Buffer0,
                                                transport=Transport,
                                                remote_ip=RemoteIp}) ->
    Buffer = handle_messages(RemoteIp, <<Buffer0/binary, Data/binary>>),
    Transport:setopts(Socket, [{active, once}]),
    {noreply, State#state{pending_buffer=Buffer}};

handle_info({tcp_closed, _Socket}, S) ->
    {stop, normal, S};

handle_info({tcp_error, _Socket, Reason}, S) ->
    ?LOG_INFO("server got tcp_error"),
    {stop, Reason, S};

handle_info(timeout, State) ->
    ?LOG_INFO("server got timeout"),
    {stop, normal, State};

handle_info(E, S) ->
    ?LOG_WARNING("server got unexpected info with msg ~w", [E]),
    {noreply, S}.

%%%===================================================================
%%% internal
%%%===================================================================

-spec handle_messages(inet:ip_address(), binary()) -> binary().
handle_messages(_, <<>>) ->
    <<>>;

handle_messages(RemoteIp, Buffer) ->
    case erlang:decode_packet(4, Buffer, []) of
        {ok, Msg, More} ->
            case Msg of
                ?SEQUENCER_MSG(Payload) ->
                    handle_request(RemoteIp, sequencer_messages:decode(Payload));
                _ ->
                    %% drop unknown messages on the floor
                    ok
            end,
            handle_messages(RemoteIp, More);
        _ ->
            Buffer
    end.

-dialyzer({no_opaque, handle_request/2}).
-spec handle_request(inet:ip_address(), sequencer_message()) -> ok.
handle_request(_, #redblue_prepare_request{tx_id=TxId, tx_label=Label, readset=RS, writeset=WS, snapshot_vc=VC}) ->
    ok = rb_sequencer:redblue_commit(self(), TxId, Label, RS, WS, VC);

handle_request(RemoteIp, #ping{partitions=Partitions}) ->
    ok = rb_sequencer:ping(self(), RemoteIp, Partitions);

handle_request(_, #reblue_put_conflicts{conflicts=Conflicts}) ->
    ok = rb_sequencer:conflicts(Conflicts);

handle_request(_, _) ->
    %% Ignore all other messages
    ok.
