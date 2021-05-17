-module(rb_sequencer_service).
-behavior(gen_server).
-include("rb_sequencer.hrl").
-include_lib("kernel/include/logger.hrl").

%% Supervisor
-export([start_link/0]).
-ignore_xref([start_link/0]).

%% init api
-export([init_leader_state/0,
         init_follower_state/0,
         learn_conflicts/1]).

%% heartbeat api
-export([prepare_heartbeat/1,
         accept_heartbeat/5,
         decide_heartbeat/3]).

%% tx API
-export([prepare/6,
         accept/9,
         decide/4,
         learn_abort/4,
         deliver/4]).

%% For debug only
-export([learn_last_delivered/0]).
-ignore_xref([learn_last_delivered/0]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2]).

-define(deliver_event, deliver_event).
-define(prune_event, prune_event).
-define(send_aborts_event, send_aborts_event).

-define(leader, leader).
-define(follower, follower).
-type role() :: ?leader | ?follower.

-type pending_message() :: {accept, red_coord_location(), ballot(), term(), tx_label(), partition_rs(), partition_ws(), vclock()}
                         | {accept_hb, replica_id(), ballot(), red_heartbeat_id(), grb_time:ts()}
                         | {deliver_transactions, ballot(), grb_time:ts(), [ red_heartbeat_id() | {term(), tx_label()} ]}.

-record(state, {
    replica_id = undefined :: replica_id() | undefined,

    %% only at leader
    heartbeat_process = undefined :: pid() | undefined,
    heartbeat_schedule_ms :: non_neg_integer(),
    heartbeat_schedule_timer = undefined :: reference() | undefined,

    last_delivered = 0 :: grb_time:ts(),

    %% How often to check for ready transactions.
    %% Only happens at the leader.
    deliver_timer = undefined :: reference() | undefined,
    deliver_interval :: non_neg_integer(),

    %% How often to prune already-delivered transactions.
    prune_timer = undefined :: reference() | undefined,
    prune_interval :: non_neg_integer(),

    %% How often does the leader send aborts?
    send_aborts_timer = undefined :: reference() | undefined,
    send_aborts_interval_ms :: non_neg_integer(),

    %% cache of last commited vector per key
    last_commit_vc_table :: cache({key(), tx_label()}, vclock()),

    %% paxos state and role
    synod_role = undefined :: role() | undefined,
    synod_state = undefined :: grb_paxos_state:t() | undefined,

    %% conflict information, who conflicts with whom
    conflict_relations :: conflict_relations(),

    %% A buffer of delayed abort messages, already encoded for sending.
    %%
    %% The leader can wait for a while before sending abort messages to followers.
    %% In the normal case, followers don't need to learn about aborted transactions,
    %% since they don't execute any delivery preconditions. This allows us to save
    %% an exchaned message during commit if we know the transactions is aborted.
    %%
    %% During recovery, it's important that the new leader knows about aborted
    %% transactions, otherwise it won't be able to deliver new transactions.
    %%
    %% A solution to this problem is to retry transactions that have been sitting
    %% in prepared for too long.
    abort_buffer_io = [] :: iodata(),

    %% The next sequence number to use when sending / receiving ordered messages.
    fifo_sequence_number = 0 :: non_neg_integer(),

    %% Store any pending messages from the leader. We must process certain messages
    %% in order, so if the incoming sequence number doesn't match the expected one,
    %% we will buffer it here until all the dependencies are satsified.
    pending_fifo_messages = #{} :: #{ non_neg_integer() => pending_message() },

    %% Store any pending abort messages here. Since these messages are batched, it doesn't
    %% make sense to assign them sequence numbers, as they are delivered later. If we receive
    %% an abort for a transaction before we receive an accept, buffer it here.
    pending_abort_messages = #{} :: #{ {term(), ballot()} => { {abort, atom()}, grb_time:ts()} }
}).

-spec start_link() -> {ok, pid()} | ignore | {error, term()}.
start_link() -> gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

-spec init_leader_state() -> ok.
init_leader_state() -> gen_server:call(?MODULE, init_leader).

-spec init_follower_state() -> ok.
init_follower_state() -> gen_server:call(?MODULE, init_follower).

-spec learn_conflicts(conflict_relations()) -> ok.
learn_conflicts(Conflicts) -> gen_server:call(?MODULE, {learn_conflicts, Conflicts}).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% heartbeat API
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec prepare_heartbeat(term()) -> ok.
prepare_heartbeat(Id) ->
    gen_server:cast(?MODULE, {prepare_hb, Id}).

-spec accept_heartbeat(replica_id(), non_neg_integer(), ballot(), red_heartbeat_id(), grb_time:ts()) -> ok.
accept_heartbeat(SourceReplica, Sequence, Ballot, Id, Ts) ->
    gen_server:cast(?MODULE, {sequence_msg, Sequence, {accept_hb, SourceReplica, Ballot, Id, Ts}}).

-spec decide_heartbeat(ballot(), term(), grb_time:ts()) -> ok.
decide_heartbeat(Ballot, Id, Ts) ->
    gen_server:cast(?MODULE, {decide_hb, Ballot, Id, Ts}).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% transaction API
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec prepare(TxId :: term(),
              Label :: tx_label(),
              PRS :: partition_rs(),
              PWS :: partition_ws(),
              SnapshotVC :: vclock(),
              Coord :: red_coord_location()) -> ok.

prepare(TxId, Label, PRS, PWS, SnapshotVC, Coord) ->
    gen_server:cast(?MODULE, {prepare, Coord, TxId, Label, PRS, PWS, SnapshotVC}).

-spec accept(Sequence :: non_neg_integer(),
             Ballot :: ballot(),
             TxId :: term(),
             Label :: tx_label(),
             RS :: partition_rs(),
             WS :: partition_ws(),
             Vote :: red_vote(),
             PrepareVC :: vclock(),
             Coord :: red_coord_location()) -> ok.

accept(Sequence, Ballot, TxId, Label, RS, WS, Vote, PrepareVC, Coord) ->
    gen_server:cast(
        ?MODULE,
        {sequence_msg, Sequence,
            {accept, Coord, Ballot, TxId, Label, RS, WS, Vote, PrepareVC}}
    ).

-spec decide(ballot(), term(), red_vote(), grb_vclock:ts()) -> ok.
decide(Ballot, TxId, Decision, CommitTs) ->
    gen_server:cast(
        ?MODULE,
        {decision, Ballot, TxId, Decision, CommitTs}
    ).

-spec learn_abort(ballot(), term(), term(), grb_vclock:ts()) -> ok.
learn_abort(Ballot, TxId, Reason, CommitTs) ->
    gen_server:cast(
        ?MODULE,
        {learn_abort, Ballot, TxId, Reason, CommitTs}
    ).

-spec deliver(non_neg_integer(), ballot(), grb_time:ts(), [ { term(), tx_label() } | red_heartbeat_id() ]) -> ok.
deliver(Sequence, Ballot, Timestamp, TransactionIds) ->
    gen_server:cast(
        ?MODULE,
        {sequence_msg, Sequence,
            {deliver_transactions, Ballot, Timestamp, TransactionIds}}
    ).

-spec learn_last_delivered() -> non_neg_integer().
learn_last_delivered() ->
    gen_server:call(?MODULE, get_last_delivered, infinity).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% gen_server callbacks
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

init(_) ->
    {ok, DeliverInterval} = application:get_env(rb_sequencer, red_delivery_interval),
    PruningInterval = application:get_env(rb_sequencer, red_prune_interval, 0),

    %% conflict information can be overwritten by calling rb_sequencer:conflicts/1
    Conflicts = application:get_env(rb_sequencer, red_conflicts_config, #{}),

    %% only at the leader, but we don't care.
    %% Peg heartbeat schedule to 1ms, we don't want the user to be able to set something smaller.
    HeartbeatScheduleMs = max(application:get_env(rb_sequencer, red_heartbeat_schedule_ms, 1), 1),
    {ok, SendAbortIntervalMs} = application:get_env(rb_sequencer, red_abort_interval_ms),

    %% This contains an entry per key with a commit snapshot
    LastCommitTable = ets:new(last_commit_table, [set, protected]),

    %% don't care about setting bad values, we will overwrite it
    State = #state{heartbeat_schedule_ms=HeartbeatScheduleMs,
                   deliver_interval=DeliverInterval,
                   prune_interval=PruningInterval,
                   send_aborts_interval_ms=SendAbortIntervalMs,
                   last_commit_vc_table=LastCommitTable,
                   synod_state=undefined,
                   conflict_relations=Conflicts},

    {ok, State}.

handle_call(get_last_delivered, _Sender, S=#state{last_delivered=LastDelivered}) ->
    {reply, LastDelivered, S};

handle_call(init_leader, _Sender, S=#state{synod_role=undefined, synod_state=undefined}) ->
    ReplicaId = rb_sequencer_dc_manager:replica_id(),
    {ok, Pid} = rb_sequencer_heartbeat:new(ReplicaId),
    {reply, ok, start_timers(S#state{replica_id=ReplicaId,
                                     heartbeat_process=Pid,
                                     synod_role=?leader,
                                     synod_state=rb_sequencer_paxos_state:new()})};

handle_call(init_follower, _Sender, S=#state{synod_role=undefined, synod_state=undefined}) ->
    ReplicaId = rb_sequencer_dc_manager:replica_id(),
    {reply, ok, start_timers(S#state{replica_id=ReplicaId,
                                     synod_role=?follower,
                                     synod_state=rb_sequencer_paxos_state:new()})};

handle_call({learn_conflicts, Conflicts}, _Sender, State) ->
    {reply, ok, State#state{conflict_relations=Conflicts}};

handle_call(E, _From, S) ->
    ?LOG_WARNING("~p unexpected call: ~p~n", [?MODULE, E]),
    {reply, ok, S}.

%%%===================================================================
%%% leader protocol messages
%%%===================================================================

handle_cast({prepare_hb, Id}, S0=#state{synod_role=?leader,
                                        synod_state=LeaderState0,
                                        fifo_sequence_number=SequenceNumber}) ->

    %% Cancel and resubmit any pending heartbeats for this partition.
    S = reschedule_heartbeat(S0),

    {Result, LeaderState} = rb_sequencer_paxos_state:prepare_hb(Id, LeaderState0),
    case Result of
        {ok, Ballot, Timestamp} ->
            ?LOG_DEBUG("HEARTBEAT_PREPARE(~b, ~p, ~b)", [Ballot, Id, Timestamp]),
            rb_sequencer_heartbeat:handle_accept_ack(Ballot, Id, Timestamp),
            lists:foreach(fun(ReplicaId) ->
                rb_sequencer_dc_connection_manager:send_red_heartbeat(ReplicaId, SequenceNumber, Ballot, Id, Timestamp)
            end, rb_sequencer_dc_connection_manager:connected_replicas());

        {already_decided, _Decision, _Timestamp} ->
            ?LOG_ERROR("heartbeat already decided, reused identifier ~p", [Id]),
            erlang:error(heartbeat_already_decided)
    end,
    {noreply, S#state{synod_state=LeaderState, fifo_sequence_number=SequenceNumber+1}};

handle_cast({prepare, Coordinator, TxId, Label, PRS, PWS, SnapshotVC},
            S0=#state{synod_role=?leader}) ->
    {noreply, prepare_internal(TxId, Label, PRS, PWS, SnapshotVC, Coordinator, S0)};

handle_cast({decide_hb, Ballot, Id, Ts}, S0=#state{synod_role=?leader}) ->
    ?LOG_DEBUG("HEARTBEAT_DECIDE(~b, ~p, ~b)", [Ballot, Id, Ts]),
    {ok, S} = decide_hb_internal(Ballot, Id, Ts, S0),
    {noreply, S};

handle_cast({decision, Ballot, TxId, Decision, CommitTs}, S0=#state{synod_role=?leader}) ->
    ?LOG_DEBUG("DECIDE(~b, ~p, ~p)", [Ballot, TxId, Decision]),
    {ok, S} = decide_internal(Ballot, TxId, Decision, CommitTs, S0),
    {noreply, maybe_buffer_abort(Ballot, TxId, Decision, CommitTs, S)};

%%%===================================================================
%%% follower protocol messages
%%%===================================================================

handle_cast({sequence_msg, SequenceNumber, Payload},
               S0=#state{synod_role=?follower, fifo_sequence_number=NextSequenceNumber, pending_fifo_messages=Pending}) ->
    S = if
        NextSequenceNumber =:= SequenceNumber ->
            %% process message, advance Seq, re-process pending
            S1 = execute_follower_command(Payload, S0),
            reprocess_pending_messages(S1#state{fifo_sequence_number=NextSequenceNumber + 1});
        true ->
            S0#state{pending_fifo_messages=Pending#{SequenceNumber => Payload}}
    end,
    {noreply, S};

handle_cast({learn_abort, Ballot, TxId, Reason, CommitTs}, S0=#state{synod_role=?follower}) ->
    AbortDecision = {abort, Reason},
    S = case decide_internal(Ballot, TxId, AbortDecision, CommitTs, S0) of
        {ok, S1} ->
            S1;
        not_prepared ->
            %% Stash for later
            Pending = S0#state.pending_abort_messages,
            S0#state{pending_abort_messages=Pending#{ {TxId, Ballot} => {AbortDecision, CommitTs} }}
    end,
    {noreply, S};

handle_cast(E, S) ->
    ?LOG_WARNING("~p unexpected cast: ~p~n", [?MODULE, E]),
    {noreply, S}.

%%%===================================================================
%%% strong message sequencer
%%%===================================================================

-spec reprocess_pending_messages(#state{}) -> #state{}.
reprocess_pending_messages(S0=#state{fifo_sequence_number=N,
                                     pending_fifo_messages=Pending0}) ->
    case maps:take(N, Pending0) of
        error ->
            S0;
        {Msg, Pending} ->
            S1 = execute_follower_command(Msg, S0),
            reprocess_pending_messages(S1#state{pending_fifo_messages=Pending, fifo_sequence_number=N+1})
    end.

-spec reprocess_pending_aborts(term(), ballot(), #state{}) -> #state{}.
reprocess_pending_aborts(TxId, Ballot, S0=#state{pending_abort_messages=PendingAborts0}) ->
    case maps:take({TxId, Ballot}, PendingAborts0) of
        error ->
            S0;
        {{Decision, CommitTs}, PendingAborts} ->
            {ok, S} = decide_internal(Ballot, TxId, Decision, CommitTs, S0),
            S#state{pending_abort_messages=PendingAborts}
    end.

-spec execute_follower_command(pending_message(), #state{}) -> #state{}.
execute_follower_command({accept, Coordinator, Ballot, TxId, Label, PRS, PWS, Vote, PrepareVC},
                         State=#state{replica_id=LocalId, synod_state=FollowerState0}) ->
    ?LOG_DEBUG("ACCEPT(~b, ~p, ~p), reply to coordinator ~p", [Ballot, TxId, Vote, Coordinator]),
    {ok, FollowerState} = rb_sequencer_paxos_state:accept(Ballot, TxId, Label, PRS, PWS, Vote, PrepareVC, FollowerState0),
    ok = reply_accept_ack(Coordinator, LocalId, Ballot, TxId, Vote, PrepareVC),
    reprocess_pending_aborts(TxId, Ballot, State#state{synod_state=FollowerState});

execute_follower_command({accept_hb, SourceReplica, Ballot, Id, Ts},
                         S0=#state{synod_state=FollowerState0}) ->

    ?LOG_DEBUG("HEARTBEAT_ACCEPT(~b, ~p, ~b)", [Ballot, Id, Ts]),
    {ok, FollowerState} = rb_sequencer_paxos_state:accept_hb(Ballot, Id, Ts, FollowerState0),
    ok = rb_sequencer_dc_connection_manager:send_red_heartbeat_ack(SourceReplica, Ballot, Id, Ts),
    S0#state{synod_state=FollowerState};

execute_follower_command({deliver_transactions, Ballot, Timestamp, TransactionIds},
                         S0=#state{last_delivered=LastDelivered, last_commit_vc_table=LastVCTable}) ->

    ValidBallot = rb_sequencer_paxos_state:deliver_is_valid_ballot(Ballot, S0#state.synod_state),
    if
        Timestamp > LastDelivered andalso ValidBallot ->
            %% We're at follower, so we will always be ready to receive a deliver event
            %% We already checked for a valid ballot above, so that can't fail.
            %% Due to FIFO, we will always receive a DELIVER after an ACCEPT from the same leader,
            %% so we don't have to worry about that either.
            {
                S1,
                LocalNodesTxList
            }
                = lists:foldl(
                    fun
                        ({?red_heartbeat_marker, _}=Id, {StateAcc, LocalAcc}) ->
                            %% heartbeats always commit
                            ?LOG_DEBUG("HEARTBEAT_DECIDE(~b, ~p, ~b)", [Ballot, Id, Timestamp]),
                            {ok, SAcc} = decide_hb_internal(Ballot, Id, Timestamp, StateAcc),
                            {
                                SAcc,
                                LocalAcc
                            };

                        ({TxId, Label}, {StateAcc, LocalAcc}) ->
                            %% We only receive committed transactions. Aborted transactions were received during decision.
                            ?LOG_DEBUG("DECIDE(~b, ~p, ~p)", [Ballot, TxId, ok]),
                            {ok, SAcc} = decide_internal(Ballot, TxId, ok, Timestamp, StateAcc),
                            %% Since it's committed, we can deliver it immediately
                            {Label, WS, CommitVC} = rb_sequencer_paxos_state:get_decided_data(TxId, SAcc#state.synod_state),
                            %% We update last_vc table here
                            ok = update_last_commit_table(Label, WS, CommitVC, LastVCTable),
                            {
                                SAcc,
                                append_to_local_delivery_tx(TxId, WS, CommitVC, LocalAcc)
                            }
                    end,
                    {S0, #{}},
                    TransactionIds
                ),

            %% Let's send a delivery to all local nodes
            maps:fold(
                fun(Node, TxList, _) ->
                    ?LOG_DEBUG("DELIVER(~p, ~w)", [LastDelivered, TxList]),
                    ok = rb_sequencer_tcp_server:send_delivery(Node, LastDelivered, TxList)
                end,
                ok,
                LocalNodesTxList
            ),

            %% We won't receive more transactions with this (or less) timestamp, so we can perform a heartbeat
            lists:foreach(
                fun(RemoteNode) ->
                    rb_sequencer_tcp_server:deliver_heartbeat(RemoteNode, LastDelivered)
                end,
                rb_sequencer_dc_manager:connected_nodes()
            ),
            S1#state{last_delivered=Timestamp};

        true ->
            ?LOG_WARNING("DELIVER(~p, ~p) is not valid", [Ballot, Timestamp]),
            S0
    end.


%%%===================================================================
%%% handle_info
%%%===================================================================

handle_info(?deliver_event, S=#state{synod_role=?leader,
                                     synod_state=SynodState,
                                     last_delivered=LastDelivered0,
                                     last_commit_vc_table=LastVCTable,
                                     deliver_timer=Timer,
                                     deliver_interval=Interval,
                                     fifo_sequence_number=StartingSeq}) ->
    ?CANCEL_TIMER_FAST(Timer),
    CurBallot = rb_sequencer_paxos_state:current_ballot(SynodState),
    {LastDelivered, NewSeq} = deliver_updates(StartingSeq, CurBallot, LastDelivered0, SynodState, LastVCTable),
    if
        LastDelivered > LastDelivered0 ->
            lists:foreach(
                fun(RemoteNode) ->
                    ?LOG_DEBUG("DELIVER_HB(~p, ~b)", [RemoteNode, LastDelivered]),
                    rb_sequencer_tcp_server:deliver_heartbeat(RemoteNode, LastDelivered)
                end,
                rb_sequencer_dc_manager:connected_nodes()
            ),
            ok;
        true ->
            ok
    end,
    {noreply, S#state{fifo_sequence_number=NewSeq,
                      last_delivered=LastDelivered,
                      deliver_timer=erlang:send_after(Interval, self(), ?deliver_event)}};

handle_info(?prune_event, S=#state{last_delivered=LastDelivered,
                                   synod_state=SynodState,
                                   prune_timer=Timer,
                                   prune_interval=Interval}) ->

    ?CANCEL_TIMER_FAST(Timer),
    ?LOG_DEBUG("PRUNE_BEFORE(~b)", [LastDelivered]),
    {noreply, S#state{synod_state=rb_sequencer_paxos_state:prune_decided_before(LastDelivered, SynodState),
                      prune_timer=erlang:send_after(Interval, self(), ?prune_event)}};

handle_info(?send_aborts_event, S=#state{synod_role=?leader,
                                         abort_buffer_io=AbortBuffer,
                                         send_aborts_timer=Timer,
                                         send_aborts_interval_ms=Interval}) ->
    ?CANCEL_TIMER_FAST(Timer),
    ok = send_abort_buffer(AbortBuffer),
    {noreply, S#state{abort_buffer_io=[],
                      send_aborts_timer=erlang:send_after(Interval, self(), ?send_aborts_event)}};

handle_info(E, S) ->
    ?LOG_WARNING("~p unexpected info: ~p~n", [?MODULE, E]),
    {noreply, S}.

%%%===================================================================
%%% internal
%%%===================================================================

-spec start_timers(#state{}) -> #state{}.
start_timers(S=#state{synod_role=?leader, deliver_interval=DeliverInt,
                      prune_interval=PruneInt, send_aborts_interval_ms=AbortInt}) ->

    reschedule_heartbeat(S#state{prune_timer=rb_sequencer_dc_utils:maybe_send_after(PruneInt, ?prune_event),
                                 deliver_timer=rb_sequencer_dc_utils:maybe_send_after(DeliverInt, ?deliver_event),
                                 send_aborts_timer=rb_sequencer_dc_utils:maybe_send_after(AbortInt, ?send_aborts_event)});

start_timers(S=#state{synod_role=?follower, prune_interval=PruneInt}) ->
    S#state{prune_timer=rb_sequencer_dc_utils:maybe_send_after(PruneInt, ?prune_event)}.

-spec reschedule_heartbeat(#state{}) -> #state{}.
reschedule_heartbeat(S=#state{heartbeat_process=HBPid,
                              heartbeat_schedule_ms=DelayMs,
                              heartbeat_schedule_timer=Timer}) ->
    if
        is_reference(Timer) -> ?CANCEL_TIMER_FAST(Timer);
        true -> ok
    end,
    S#state{heartbeat_schedule_timer=rb_sequencer_heartbeat:schedule_heartbeat(HBPid, DelayMs)}.

-spec prepare_internal(term(), tx_label(), partition_rs(), partition_ws(), vclock(), red_coord_location(), #state{}) -> #state{}.
prepare_internal(TxId, Label, PRS, PWS, SnapshotVC, Coordinator,
                 S0=#state{replica_id=LocalId,
                           synod_state=LeaderState0, conflict_relations=Conflicts,
                           last_commit_vc_table=LastCommitTable,
                           fifo_sequence_number=SequenceNumber}) ->

    %% Cancel and resubmit any pending heartbeats for this partition.
    S1 = reschedule_heartbeat(S0),
    {Result, LeaderState} =
        rb_sequencer_paxos_state:prepare(
            TxId, Label, PRS, PWS, SnapshotVC, LastCommitTable, Conflicts, LeaderState0
        ),
    ?LOG_DEBUG("~p prepared as ~p, reply to coordinator ~p", [TxId, Result, Coordinator]),
    case Result of
        {already_decided, Decision, CommitVC} ->
            %% skip replicas, this is enough to reply to the client
            reply_already_decided(Coordinator, LocalId, TxId, Decision, CommitVC),
            S1#state{synod_state=LeaderState};

        {Vote, Ballot, PrepareVC} ->
            ok = reply_accept_ack(Coordinator, LocalId, Ballot, TxId, Vote, PrepareVC),
            ok = send_accepts(Coordinator, SequenceNumber, Ballot, TxId, Label, Vote, PRS, PWS, PrepareVC),
            S1#state{synod_state=LeaderState, fifo_sequence_number=SequenceNumber + 1}
    end.

-spec decide_hb_internal(ballot(), term(), rb_sequencer_time:ts(), #state{}) -> {ok, #state{}}.
-dialyzer({nowarn_function, decide_hb_internal/4}).
decide_hb_internal(Ballot, Id, Ts, S=#state{synod_state=SynodState0}) ->
    case rb_sequencer_paxos_state:decision_hb(Ballot, Id, Ts, SynodState0) of
        {ok, SynodState} ->
            {ok, S#state{synod_state=SynodState}};

        bad_ballot ->
            ?LOG_ERROR("bad heartbeat ballot ~b", [Ballot]),
            {ok, S};

        not_prepared ->
            ?LOG_ERROR("out-of-order decision (~b) for a not prepared transaction ~p", [Ballot, Id]),
            {ok, S}
    end.

-spec decide_internal(ballot(), term(), red_vote(), grb_time:ts(), #state{}) -> {ok, #state{}} | not_prepared.
decide_internal(Ballot, TxId, Decision, CommitTs, S=#state{synod_state=SynodState0}) ->
    case rb_sequencer_paxos_state:decision(Ballot, TxId, Decision, CommitTs, SynodState0) of
        {ok, SynodState} ->
            {ok, S#state{synod_state=SynodState}};

        not_prepared ->
            not_prepared;

        bad_ballot ->
            ?LOG_ERROR("~bad ballot ~b for ~p", [Ballot, TxId]),
            {ok, S}
    end.

-spec maybe_buffer_abort(ballot(), term(), red_vote(), grb_time:ts(), #state{}) -> #state{}.
maybe_buffer_abort(_Ballot, _TxId, ok, _CommitTs, State) ->
    %% If this is a commit, we can wait until delivery
    State;
maybe_buffer_abort(Ballot, TxId, {abort, Reason}, CommitTs, State=#state{abort_buffer_io=AbortBuffer,
                                                                         send_aborts_interval_ms=Ms}) ->
    FramedAbortMsg = rb_sequencer_dc_messages:frame(rb_sequencer_dc_messages:red_learn_abort(Ballot, TxId, Reason, CommitTs)),
    if
        Ms > 0 ->
            %% Abort delay is active.
            State#state{abort_buffer_io=[AbortBuffer, FramedAbortMsg]};
        true ->
            %% Abort delay is disabled, send immediately.
            ok = send_abort_buffer(FramedAbortMsg),
            State
    end.

-spec reply_accept_ack(red_coord_location(), replica_id(), ballot(), term(), red_vote(), vclock()) -> ok.
reply_accept_ack(CoordReplica, MyReplica, Ballot, TxId, Vote, PrepareVC) ->
    if
        CoordReplica =:= MyReplica ->
            rb_sequencer_coordinator:accept_ack(Ballot, TxId, Vote, PrepareVC);
        true ->
            rb_sequencer_dc_connection_manager:send_red_accept_ack(CoordReplica, Ballot, TxId, Vote, PrepareVC)
    end.

-spec reply_already_decided(red_coord_location(), replica_id(), term(), red_vote(), vclock()) -> ok.
reply_already_decided(CoordReplica, MyReplica, TxId, Decision, CommitVC) ->
    if
        CoordReplica =:= MyReplica ->
            rb_sequencer_coordinator:already_decided(TxId, Decision, CommitVC);
        true ->
            rb_sequencer_dc_connection_manager:send_red_already_decided(CoordReplica, TxId, Decision, CommitVC)
    end.

-spec send_accepts(Coordinator :: red_coord_location(),
                   SequenceNumber :: non_neg_integer(),
                   Ballot :: ballot(),
                   TxId :: term(),
                   Label :: tx_label(),
                   Decision :: red_vote(),
                   PRS :: partition_rs(),
                   PWS :: partition_ws(),
                   PrepareVC :: vclock()) -> ok.

send_accepts(Coordinator, SequenceNumber, Ballot, TxId, Label, Decision, RS, WS, PrepareVC) ->
    AcceptMsgIO = rb_sequencer_dc_messages:frame(rb_sequencer_dc_messages:red_accept(SequenceNumber, Coordinator, Ballot, Decision, TxId, Label, RS, WS, PrepareVC)),
    lists:foreach(
        fun(R) -> rb_sequencer_dc_connection_manager:send_raw_framed(R, AcceptMsgIO) end,
        rb_sequencer_dc_connection_manager:connected_replicas()
    ).

-spec send_abort_buffer(iodata()) -> ok.
send_abort_buffer([]) ->
    ok;
send_abort_buffer(IOAborts) ->
    lists:foreach(fun(ReplicaId) ->
        rb_sequencer_dc_connection_manager:send_raw_framed(ReplicaId, IOAborts)
    end, rb_sequencer_dc_connection_manager:connected_replicas()).

deliver_updates(SequenceNumber, Ballot, From, SynodState, LastVCTable) ->
    {NewFrom, NewSeq, RemoteIOList, LocalIOListMap} =
        deliver_updates(
            SequenceNumber,
            Ballot,
            From,
            SynodState,
            LastVCTable,
            [],
            #{}
    ),

    lists:foreach(
        fun(ReplicaId) ->
            rb_sequencer_dc_connection_manager:send_raw_framed(ReplicaId, RemoteIOList)
        end,
        rb_sequencer_dc_connection_manager:connected_replicas()
    ),

    maps:fold(
        fun(Node, Msg, _) ->
            ?LOG_DEBUG("DELIVER(~p, [...])", [Node]),
            ok = rb_sequencer_tcp_server:send_msg(Node, Msg)
        end,
        ok,
        LocalIOListMap
    ),

    {NewFrom, NewSeq}.

-spec deliver_updates(
    non_neg_integer(),
    ballot(),
    non_neg_integer(),
    rb_sequencer_paxos_state:t(),
    cache({key(), tx_label()}, vclock()),
    iodata(),
    #{inet:ip_address() => iodata()}
) -> {non_neg_integer(), non_neg_integer(), iodata(), #{inet:ip_address() => iodata()}}.

deliver_updates(SequenceNumber, Ballot, From, SynodState, LastVCTable, RemoteIOAcc, LocalIOMap) ->
    case rb_sequencer_paxos_state:get_next_ready(From, SynodState) of
        false ->
            {From, SequenceNumber, RemoteIOAcc, LocalIOMap};

        {NextFrom, Entries} ->
            %% Collect only the identifiers for the transactions, we don't care
            %% about the writeset, followers already have it.
            %%
            %% No need to reverse the accumulator, since they all have the same
            %% commit timestamp, they can be delivered in any order.
            {
                IdentifiersForRemoteReplicas,
                LocalNodesTxList
            }
                = lists:foldl(
                    fun
                        ( {?red_heartbeat_marker, _}=Heartbeat, {RemoteAcc, LocalAcc}) ->
                            {
                                [ Heartbeat | RemoteAcc ],
                                LocalAcc
                            };

                        ({TxId, Label, WriteSet, CommitVC}, {RemoteAcc, LocalAcc}) ->
                            if
                                is_map(WriteSet) andalso map_size(WriteSet) > 0 ->
                                    %% We update last_vc table here
                                    ok = update_last_commit_table(Label, WriteSet, CommitVC, LastVCTable),
                                    {
                                        [ { TxId, Label } | RemoteAcc ],
                                        append_to_local_delivery_tx(TxId, WriteSet, CommitVC, LocalAcc)
                                    };
                                true ->
                                    {
                                        [ { TxId, Label } | RemoteAcc ],
                                        LocalAcc
                                    }
                            end
                    end,
                    {[], #{}},
                    Entries
                ),

            Msg =
                rb_sequencer_dc_messages:frame(
                    rb_sequencer_dc_messages:red_deliver(SequenceNumber, Ballot, NextFrom, IdentifiersForRemoteReplicas)
                ),

            deliver_updates(
                SequenceNumber + 1,
                Ballot,
                NextFrom,
                SynodState,
                LastVCTable,
                [RemoteIOAcc, Msg],
                append_to_iodata_map(NextFrom, LocalNodesTxList, LocalIOMap)
            )
    end.

-spec update_last_commit_table(
    tx_label(),
    partition_ws(),
    vclock(),
    cache({key(), tx_label()}, vclock())
) -> ok.

update_last_commit_table(Label, PWS, CommitVC, LastVCTable) ->
    maps:fold(
        fun(_, WS, _) ->
            maps:fold(
                fun(Key, _, _) ->
                    update_last_vc(Key, Label, CommitVC, LastVCTable)
                end,
                ok,
                WS
            )
        end,
        ok,
        PWS
    ).

%% LastVC contains, for each key and transaction label, its max commit vector.
%% Although maxing two vectors on each transaction could be slow, this only happens on red transactions,
%% which are already slow due to cross-dc 2PC. Adding a little bit of time doing this max shouldn't add
%% too much overhead on top.
-spec update_last_vc(key(), tx_label(), vclock(), cache({key(), tx_label()}, vclock())) -> ok.
update_last_vc(Key, Label, CommitVC, Table) ->
    case ets:lookup(Table, {Key, Label}) of
        [{_, LastVC}] ->
            true = ets:update_element(Table, {Key, Label},
                                      {2, grb_vclock:max(LastVC, CommitVC)});
       [] ->
           true = ets:insert(Table, {{Key, Label}, CommitVC})
    end,
    ok.

-spec append_to_local_delivery_tx(
    term(),
    partition_ws(),
    vclock(),
    #{inet:ip_address() => [{term(), partition_ws(), vclock()}]}
) -> #{inet:ip_address() => [{term(), partition_ws(), vclock()}]}.

%% Append this transaction to all the buckets needed for delivery
append_to_local_delivery_tx(TxId, PWS, CommitVC, LocalMsgs) ->
    Entry = {TxId, PWS, CommitVC},
    lists:foldl(
        fun(Partition, Acc) ->
            Node = rb_sequencer_dc_manager:partition_node(Partition),
            case Acc of
                #{ Node := PrevTxs } ->
                    Acc#{Node => [Entry | PrevTxs]};
                _ ->
                    Acc#{Node => [Entry]}
            end
        end,
        LocalMsgs,
        maps:keys(PWS)
    ).

-spec append_to_iodata_map(
    Ts :: rb_sequencer_time:ts(),
    TxListMap :: #{inet:ip_address() => [{term(), partition_ws(), vclock()}]},
    IOAcc :: #{inet:ip_address() => iodata()}) -> #{inet:ip_address() => iodata()}.

%% Add to the local accumulator for deliver messages, already serialized
append_to_iodata_map(Timestamp, TxListMap, IOAcc) ->
    maps:fold(
        fun(Node, TxList, InnerIOAcc) ->
            Msg = sequencer_messages:frame(4, sequencer_messages:deliver(Timestamp, TxList)),
            case InnerIOAcc of
                #{ Node := IOData } -> InnerIOAcc#{Node => [IOData, Msg]};
                _ -> InnerIOAcc#{Node => [Msg]}
            end
        end,
        IOAcc,
        TxListMap
    ).
