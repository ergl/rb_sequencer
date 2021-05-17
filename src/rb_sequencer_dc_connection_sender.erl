-module(rb_sequencer_dc_connection_sender).
-behavior(gen_server).
-include("rb_sequencer.hrl").
-include_lib("kernel/include/logger.hrl").

-export([start_link/3]).
-ignore_xref([start_link/3]).

-export([start_connection/3,
         send_msg/2,
         close/1]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2]).

-define(EXPAND_SND_BUF_INTERVAL, 100).

-record(state, {
    connected_dc :: replica_id(),
    socket :: gen_tcp:socket(),
    expand_buffer_timer :: reference()
}).

-record(handle, { pid :: pid()  }).
-type t() :: #handle{}.
-export_type([t/0]).

start_link(TargetReplica, Ip, Port) ->
    gen_server:start_link(?MODULE, [TargetReplica, Ip, Port], []).

-spec start_connection(replica_id(), inet:ip_address(), inet:port_number()) -> {ok, t()}.
start_connection(TargetReplica, Ip, Port) ->
    {ok, Pid} = rb_sequencer_dc_connection_sender_sup:start_connection_child(TargetReplica, Ip, Port),
    establish_connection(Pid).

-spec send_msg(t(), iolist()) -> ok.
send_msg(#handle{pid=Pid}, Msg) ->
    gen_server:cast(Pid, {send, Msg}).

-spec close(t()) -> ok.
close(#handle{pid=Pid}) ->
    gen_server:cast(Pid, stop).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% gen_server callbacks
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

init([TargetReplica, Ip, Port]) ->
    Opts = lists:keyreplace(packet, 1, ?INTER_DC_SOCK_OPTS, {packet, raw}),
    case gen_tcp:connect(Ip, Port, Opts) of
        {error, Reason} ->
            {stop, Reason};
        {ok, Socket} ->
            ok = expand_snd_buf(Socket),
            {ok, #state{connected_dc=TargetReplica,
                        socket=Socket,
                        expand_buffer_timer=erlang:send_after(?EXPAND_SND_BUF_INTERVAL, self(), expand_snd_buf)}}
    end.

handle_call(E, _From, S) ->
    ?LOG_WARNING("~p unexpected call: ~p~n", [?MODULE, E]),
    {reply, ok, S}.

handle_cast({send, Msg}, State=#state{socket=Socket}) ->
    ok = gen_tcp:send(Socket, Msg),
    {noreply, State};

handle_cast(stop, S) ->
    {stop, normal, S};

handle_cast(E, S) ->
    ?LOG_WARNING("~p unexpected cast: ~p~n", [?MODULE, E]),
    {noreply, S}.

handle_info({tcp, Socket, Data}, State=#state{socket=Socket, connected_dc=Target}) ->
    ?LOG_INFO("~p: Received unexpected data ~p", [?MODULE, Target, Data]),
    ok = inet:setopts(Socket, [{active, once}]),
    {noreply, State};

handle_info({tcp_closed, _Socket}, S=#state{connected_dc=R}) ->
    ?LOG_INFO("Connection lost to ~p, removing reference", [R]),
    {stop, normal, S};

handle_info({tcp_error, _Socket, Reason}, S=#state{connected_dc=R}) ->
    ?LOG_INFO("Connection errored to ~p, removing reference", [R]),
    {stop, Reason, S};

handle_info(expand_snd_buf, S=#state{expand_buffer_timer=Timer}) ->
    ?CANCEL_TIMER_FAST(Timer),
    ok = expand_snd_buf(S#state.socket),
    {noreply, S#state{expand_buffer_timer=erlang:send_after(?EXPAND_SND_BUF_INTERVAL, self(), expand_snd_buf)}};

handle_info(Info, State) ->
    ?LOG_WARNING("~p Unhandled msg ~p", [?MODULE, Info]),
    {noreply, State}.

terminate(_Reason, #state{socket=Socket, connected_dc=Replica}) ->
    ok = gen_tcp:close(Socket),
    ok = rb_sequencer_dc_connection_manager:close(Replica),
    ok.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% internal
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec establish_connection(pid()) -> {ok, t()}.
establish_connection(Pid) ->
    Handle = #handle{pid=Pid},
    ok = send_msg(
        Handle,
        rb_sequencer_dc_messages:frame(rb_sequencer_dc_messages:ping(rb_sequencer_dc_manager:replica_id()))
    ),
    {ok, Handle}.

%% Expand the send buffer size from time to time.
%% This will eventually settle in a stable state if the sndbuf stops changing.
-spec expand_snd_buf(gen_tcp:socket()) -> ok.
expand_snd_buf(Socket) ->
    case inet:getopts(Socket, [sndbuf]) of
        {error, _} ->
            %% Socket might have closed
            ok;

        {ok, [{sndbuf, SndBuf}]} ->
            case inet:setopts(Socket, [{sndbuf, SndBuf * 2}]) of
                {error, _} ->
                    %% Perhaps there's no room to continue to expand, keep it the way it was
                    inet:setopts(Socket, [{sndbuf, SndBuf}]);
                ok ->
                    ok
            end
    end.
