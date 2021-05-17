-module(rb_sequencer).
-include("rb_sequencer.hrl").
-include_lib("kernel/include/logger.hrl").

%% External API for console use
-export([start/0,
         stop/0]).

%% Called by rel
-ignore_xref([start/0,
              stop/0]).

-export([ping/3,
         conflicts/1,
         redblue_commit/6]).

%% Public API

-spec start() -> {ok, _} | {error, term()}.
start() ->
    application:ensure_all_started(rb_sequencer).

-spec stop() -> ok | {error, term()}.
stop() ->
    application:stop(rb_sequencer).

-spec ping(rb_sequencer_tcp_server:t(), inet:ip_address(), [partition_id()]) -> ok.
ping(Connection, PeerIP, Partitions) ->
    rb_sequencer_dc_manager:save_partition_information(Connection, PeerIP, Partitions).

-spec conflicts(conflict_relations()) -> ok.
conflicts(Conflicts) ->
    rb_sequencer_service:learn_conflicts(Conflicts).

-spec redblue_commit(rb_sequencer_tcp_server:t(), term(), tx_label(), partition_rs(), partition_ws(), vclock()) -> ok.
redblue_commit(Conn, TxId, Label, RS, WS, VC) ->
    Coordinator = rb_sequencer_service_manager:register_coordinator(TxId),
    rb_sequencer_coordinator:commit(Coordinator, Conn, TxId, Label, RS, WS, VC).
