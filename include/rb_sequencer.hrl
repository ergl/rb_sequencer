%% For most purposes, we don't care if the cancelled timer is in the past, or ensure
%% that the timer is really cancelled before proceeding.
-define(CANCEL_TIMER_FAST(__TRef), erlang:cancel_timer(__TRef, [{async, true}, {info, false}])).

-type partition_id() :: non_neg_integer().

%% Storage
-type cache_id() :: ets:tab().
-type cache(_Map) :: ets:tab().
-type cache(_K, _V) :: cache(#{_K := _V}).

-type replica_id() :: term().
-type all_replica_id() :: red | replica_id().
-type vclock() :: grb_vclock:vc(all_replica_id()).
%% the entry for red transactions in the clock
-define(RED_REPLICA, red).

%% Opaque types
-type key() :: term().
-type snapshot() :: term().
-type crdt() :: grb_crdt:crdt().
-type operation() :: grb_crdt:operation().

-type tx_label() :: binary().
-type conflict_relations() :: #{tx_label() := tx_label()}.

-type partition_rs() :: #{partition_id() := readset()}.
-type partition_ws() :: #{partition_id() := writeset()}.
-type readset() :: [key()].
-type writeset() :: #{key() => operation()}.
-type tx_entry() :: grb_blue_commit_log:entry().

%% Describes the current replica, consumed by other replicas (as a whole)
-record(replica_descriptor, {
    replica_id :: replica_id(),
    sequencer_ip :: inet:ip_address(),
    sequencer_port :: inet:port_number()
}).

-type replica_descriptor() :: #replica_descriptor{}.

-define(INTER_DC_SOCK_PACKET_OPT, 4).
-define(INTER_DC_SOCK_OPTS, [binary,
                             {active, once},
                             {deliver, term},
                             {packet, ?INTER_DC_SOCK_PACKET_OPT},
                             {nodelay, true}]).

-type red_coordinator() :: pid().
-type red_vote() :: ok | {abort, atom()}.

-type ballot() :: non_neg_integer().

-type red_coord_location() :: replica_id().

%% The location of a red leader
-type leader_location() :: replica_id().

-define(red_heartbeat_marker, heartbeat).
-type red_heartbeat_id() :: {?red_heartbeat_marker, non_neg_integer()}.

-export_type([partition_id/0,
              cache_id/0,
              cache/1,
              cache/2,
              partition_rs/0,
              partition_ws/0,
              readset/0,
              writeset/0,
              tx_entry/0,
              replica_id/0,
              all_replica_id/0,
              vclock/0,
              key/0,
              crdt/0,
              snapshot/0,
              operation/0,
              tx_label/0,
              conflict_relations/0,
              replica_descriptor/0,
              red_coordinator/0,
              red_vote/0,
              ballot/0,
              leader_location/0,
              red_coord_location/0,
              red_heartbeat_id/0]).
