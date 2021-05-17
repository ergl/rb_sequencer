-module(rb_sequencer_heartbeat).
-behavior(gen_server).
-include("rb_sequencer.hrl").
-include_lib("kernel/include/logger.hrl").

%% API
-export([new/1,
         schedule_heartbeat/2,
         handle_accept_ack/3]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2]).

-define(red_hb, red_heartbeat).
-define(red_fixed_hb, red_fixed_heartbeat).

-record(active_hb, {
    timestamp = undefined :: grb_time:ts() | undefined,
    ballot = undefined :: ballot() | undefined,
    to_ack :: pos_integer()
}).
-type active_heartbeats() :: #{red_heartbeat_id() := #active_hb{}}.

-record(state, {
    replica :: replica_id(),
    quorum_size :: non_neg_integer(),
    next_hb_id = {?red_heartbeat_marker, 0} :: red_heartbeat_id(),

    %% Active heartbeats accumulator
    active_heartbeats = #{} :: active_heartbeats(),

    %% For the fixed-schedule heartbeat
    fixed_interval :: non_neg_integer(),
    fixed_timer :: reference()
}).

-spec new(replica_id()) -> {ok, pid()} | ignore | {error, term()}.
new(ReplicaId) ->
    gen_server:start({local, ?MODULE}, ?MODULE, [ReplicaId], []).

-spec schedule_heartbeat(pid(), non_neg_integer()) -> reference().
schedule_heartbeat(Pid, TimeoutMs) ->
    erlang:send_after(TimeoutMs, Pid, ?red_hb).

-spec handle_accept_ack(ballot(), term(), grb_time:ts()) -> ok.
handle_accept_ack(Ballot, Id, Ts) ->
    gen_server:cast(?MODULE, {accept_ack, Ballot, Id, Ts}).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% gen_server callbacks
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

init([ReplicaId]) ->
    QuorumSize = rb_sequencer_service_manager:quorum_size(),
    %% Peg heartbeat schedule to 1ms, we don't want the user to be able to set something smaller.
    FixedInterval = max(application:get_env(rb_sequencer, red_heartbeat_fixed_schedule_ms, 1), 1),
    State = #state{replica=ReplicaId,
                   quorum_size=QuorumSize,
                   fixed_interval=FixedInterval,
                   fixed_timer=erlang:send_after(FixedInterval, self(), ?red_fixed_hb)},
    {ok, State}.

handle_call(E, _From, S) ->
    ?LOG_WARNING("~p unexpected call: ~p~n", [?MODULE, E]),
    {reply, ok, S}.

handle_cast({accept_ack, InBallot, HeartbeatId, InTimestamp}, S0=#state{active_heartbeats=ActiveHeartbeats}) ->
    S = case maps:get(HeartbeatId, ActiveHeartbeats, undefined) of
        undefined ->
            %% ignore ACCEPT_ACK from past heartbeats
            S0;
        HeartBeatState ->
            ?LOG_DEBUG("received TIMER_ACK(~b, ~p, ~b)", [InBallot, HeartbeatId, InTimestamp]),
            S0#state{active_heartbeats=handle_ack(HeartbeatId,
                                                  InBallot,
                                                  InTimestamp,
                                                  HeartBeatState,
                                                  ActiveHeartbeats)}
    end,
    {noreply, S};

handle_cast(E, S) ->
    ?LOG_WARNING("~p unexpected cast: ~p~n", [?MODULE, E]),
    {noreply, S}.

handle_info(?red_hb, State) ->
    {noreply, send_heartbeat(State)};

handle_info(?red_fixed_hb, S0=#state{fixed_timer=FixedTimer,
                                     fixed_interval=FixedInterval}) ->
    ?CANCEL_TIMER_FAST(FixedTimer),
    S = send_heartbeat(S0),
    {noreply, S#state{fixed_timer=erlang:send_after(FixedInterval, self(), ?red_fixed_hb)}};

handle_info(E, S) ->
    ?LOG_WARNING("~p unexpected info: ~p~n", [?MODULE, E]),
    {noreply, S}.

-spec send_heartbeat(#state{}) -> #state{}.
send_heartbeat(S=#state{quorum_size=QuorumSize,
                        next_hb_id=HeartbeatId, active_heartbeats=Heartbeats}) ->
    ok = rb_sequencer_service:prepare_heartbeat(HeartbeatId),
    S#state{next_hb_id=next_heartbeat_id(HeartbeatId),
            active_heartbeats=Heartbeats#{HeartbeatId => #active_hb{to_ack=QuorumSize}}}.

-spec next_heartbeat_id(red_heartbeat_id()) -> red_heartbeat_id().
next_heartbeat_id({?red_heartbeat_marker, N}) -> {?red_heartbeat_marker, N + 1}.

-spec handle_ack(HeartbeatId :: red_heartbeat_id(),
                 InBallot :: ballot(),
                 InTimestamp :: grb_time:ts(),
                 HeartbeatState :: #active_hb{},
                 ActiveHeartbeats :: active_heartbeats()) -> active_heartbeats().

handle_ack(HeartbeatId, InBallot, InTimestamp, HeartbeatState, ActiveHeartbeats) ->
    #active_hb{timestamp=Timestamp0, ballot=Ballot0, to_ack=ToAck0} = HeartbeatState,
    {ok, Ballot} = check_ballot(InBallot, Ballot0),
    {ok, Timestamp} = check_timestamp(InTimestamp, Timestamp0),
    case ToAck0 of
        N when N > 1 ->
            ActiveHeartbeats#{HeartbeatId =>
                HeartbeatState#active_hb{ballot=Ballot, timestamp=Timestamp, to_ack=ToAck0 - 1}};
        1 ->
            ?LOG_DEBUG("decided heartbeat ~w with timestamp ~b", [HeartbeatId, Timestamp]),
            %% We're always colocated in the same index node as the leader.
            ok = rb_sequencer_service:decide_heartbeat(Ballot, HeartbeatId, Timestamp),
            maps:remove(HeartbeatId, ActiveHeartbeats)
    end.

-spec check_ballot(InBallot :: ballot(),
                   Ballot :: ballot() | undefined) -> {ok, ballot()} | error.
check_ballot(InBallot, undefined) -> {ok, InBallot};
check_ballot(InBallot, Ballot) when InBallot =:= Ballot -> {ok, Ballot};
check_ballot(_, _) -> error.

-spec check_timestamp(grb_time:ts(), grb_time:ts() | undefined) -> {ok, grb_time:ts()} | error.
check_timestamp(Ts, undefined) -> {ok, Ts};
check_timestamp(Ts, Ts) -> {ok, Ts};
check_timestamp(_, _) -> error.
