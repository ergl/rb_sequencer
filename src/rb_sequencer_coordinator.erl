-module(rb_sequencer_coordinator).
-behavior(gen_server).
-include("rb_sequencer.hrl").
-include("dc_messages.hrl").
-include_lib("kernel/include/logger.hrl").

-export([start_link/1]).
-ignore_xref([start_link/1]).

-export([commit/7]).

-export([accept_ack/4,
         already_decided/3]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2]).

-record(active_tx, {
    promise :: rb_sequencer_tcp_server:t(),
    ballot = undefined :: ballot() | undefined,
    to_ack :: pos_integer(),
    result = undefined :: {red_vote(), vclock()} | undefined
}).

-type active_transactions() :: #{term() := #active_tx{}}.

-record(state, {
    self_pid :: pid(),
    replica :: replica_id(),
    quorum_size :: non_neg_integer(),
    active_transactions = #{} :: active_transactions()
}).

-spec start_link(non_neg_integer()) -> {ok, pid()}.
start_link(Id) ->
    Name = {local, generate_coord_name(Id)},
    gen_server:start_link(Name, ?MODULE, [Id], []).

-spec generate_coord_name(non_neg_integer()) -> atom().
generate_coord_name(Id) ->
    BinId = integer_to_binary(Id),
    rb_sequencer_dc_utils:safe_bin_to_atom(<<"grb_red_coordinator_", BinId/binary>>).

-spec commit(Coordinator :: red_coordinator(),
             Conn :: rb_sequencer_tcp_server:t(),
             TxId :: term(),
             Label :: tx_label(),
             PRS :: partition_rs(),
             PWS :: partition_ws(),
             VC :: vclock()) -> ok.

commit(Coordinator, Conn, TxId, Label, PRS, PWS, VC) ->
    gen_server:cast(Coordinator, {commit, Conn, TxId, Label, PRS, PWS, VC}).

-spec accept_ack(ballot(), term(), red_vote(), vclock()) -> ok.
accept_ack(Ballot, TxId, Vote, AcceptVC) ->
    case rb_sequencer_service_manager:transaction_coordinator(TxId) of
        error ->
            ok;

        {ok, Coordinator} ->
            gen_server:cast(Coordinator, {accept_ack, Ballot, TxId, Vote, AcceptVC})
    end.

-spec already_decided(term(), red_vote(), vclock()) -> ok.
already_decided(TxId, Vote, VoteVC) ->
    case rb_sequencer_service_manager:transaction_coordinator(TxId) of
        error ->
            ok;
        {ok, Coordinator} ->
            gen_server:cast(Coordinator, {already_decided, TxId, Vote, VoteVC})
    end.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% gen_server callbacks
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

init(_WorkerArgs) ->
    process_flag(trap_exit, true),
    LocalId = rb_sequencer_dc_manager:replica_id(),
    QuorumSize = rb_sequencer_service_manager:quorum_size(),
    {ok, #state{self_pid=self(),
                replica=LocalId,
                quorum_size=QuorumSize}}.

handle_call(E, _From, S) ->
    ?LOG_WARNING("~p unexpected call: ~p~n", [?MODULE, E]),
    {reply, ok, S}.

handle_cast({commit, Conn, TxId, Label, PRS, PWS, VC}, S0) ->
    {noreply, init_tx_and_send(Conn, TxId, Label, PRS, PWS, VC, S0)};

handle_cast({accept_ack, InBallot, TxId, Vote, AcceptVC}, S0=#state{self_pid=Pid,
                                                                    replica=LocalId,
                                                                    active_transactions=ActiveTransactions}) ->
    S = case maps:get(TxId, ActiveTransactions, undefined) of
        undefined ->
            %% ignore ACCEPT_ACK from past transactions
            S0;
        TxState ->
            S0#state{active_transactions=handle_ack(Pid, LocalId, TxId, InBallot, Vote, AcceptVC, TxState, ActiveTransactions)}
    end,
    {noreply, S};

handle_cast({already_decided, TxId, Vote, CommitVC}, S0=#state{self_pid=Pid, active_transactions=ActiveTransactions0}) ->
    S =
        case maps:take(TxId, ActiveTransactions0) of
            error ->
                ?LOG_DEBUG("missed ALREADY_DECIDED(~p, ~p)", [TxId, Vote]),
                S0;
            {#active_tx{promise=Connection}, ActiveTransactions} ->
                ok = reply_to_client(Connection, TxId, Vote, CommitVC),
                ok = rb_sequencer_service_manager:unregister_coordinator(TxId, Pid),
                S0#state{active_transactions=ActiveTransactions}
        end,
    {noreply, S};

handle_cast(E, S) ->
    ?LOG_WARNING("~p unexpected cast: ~p~n", [?MODULE, E]),
    {noreply, S}.

handle_info(Info, State) ->
    ?LOG_WARNING("~p Unhandled msg ~p", [?MODULE, Info]),
    {noreply, State}.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% internal
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

init_tx_and_send(Conn, TxId, Label, PRS, PWS, VC, S=#state{replica=LocalId,
                                                           quorum_size=QuorumSize,
                                                           active_transactions=Transactions}) ->
    LeaderReplica = rb_sequencer_service_manager:leader(),
    if
        LeaderReplica =:= LocalId ->
            %% local send
            ok = rb_sequencer_service:prepare(TxId, Label, PRS, PWS, VC, LocalId);
        true ->
            %% remote send
            ok = rb_sequencer_dc_connection_manager:send_red_prepare(
                LeaderReplica,
                LocalId,
                TxId,
                Label,
                PRS,
                PWS,
                VC
            )
    end,
    S#state{active_transactions=Transactions#{TxId => #active_tx{promise=Conn, to_ack=QuorumSize}}}.

-spec handle_ack(pid(), replica_id(), term(), ballot(), red_vote(), vclock(), #active_tx{}, active_transactions()) -> active_transactions().
handle_ack(CoordPid, LocalId, TxId, InBallot, InVote, InAcceptVC, TxState, ActiveTransactions) ->
    #active_tx{ballot=Ballot0, to_ack=ToAck0, result=Result0} = TxState,
    {ok, Ballot} = check_ballot(InBallot, Ballot0),
    {ok, Vote, AcceptVC} = check_decision(InVote, InAcceptVC, Result0),
    case ToAck0 of
        N when N > 1 ->
            ActiveTransactions#{TxId =>
                TxState#active_tx{ballot=Ballot, result={Vote, AcceptVC}, to_ack=ToAck0 - 1}};
        1 ->

            ok = reply_to_client(TxState#active_tx.promise, TxId, Vote, AcceptVC),

            CommitTs = grb_vclock:get_time(?RED_REPLICA, AcceptVC),
            case rb_sequencer_service_manager:leader() of
                LocalId ->
                    %% local decision
                    ok = rb_sequencer_service:decide(Ballot, TxId, Vote, CommitTs);
                LeaderReplica ->
                    %% remote decision
                    ok = rb_sequencer_dc_connection_manager:send_red_decision(
                        LeaderReplica,
                        Ballot,
                        TxId,
                        Vote,
                        CommitTs
                    )
            end,

            ok = rb_sequencer_service_manager:unregister_coordinator(TxId, CoordPid),
            maps:remove(TxId, ActiveTransactions)
    end.

-spec check_ballot(InBallot :: ballot(),
                   Ballot :: ballot() | undefined) -> {ok, ballot()} | error.
check_ballot(InBallot, undefined) -> {ok, InBallot};
check_ballot(InBallot, Ballot) when InBallot =:= Ballot -> {ok, Ballot};
check_ballot(_, _) -> error.

-spec check_decision(red_vote(), vclock(), {red_vote(), vclock()} | undefined) -> {ok, red_vote(), vclock()} | error.
check_decision(Vote, Clock, undefined) -> {ok, Vote, Clock};
check_decision(Vote, Clock, {Vote, Clock}) -> {ok, Vote, Clock};
check_decision(_, _, _) -> error.

-spec reply_to_client(grb_tcp_server:t(), term(), red_vote(), vclock()) -> ok.
reply_to_client(Connection, TxId, Vote, CommitVC) ->
    Outcome =
        case Vote of
            ok -> {ok, CommitVC};
            {abort, Reason} -> {abort, Reason}
        end,
    rb_sequencer_tcp_server:send_commit_response(
        Connection,
        TxId,
        Outcome
    ).
