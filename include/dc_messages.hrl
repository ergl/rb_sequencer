-define(VERSION, 0).
-define(VERSION_BYTES, 1).
-define(VERSION_BITS, (?VERSION_BYTES * 8)).

%% Serialize messages as ints instead of records
-define(MSG_KIND_BITS, 8).

%% Red transactions
-define(RED_PREPARE_KIND, 0).
-define(RED_ACCEPT_KIND, 1).
-define(RED_ACCEPT_ACK_KIND, 2).
-define(RED_DECIDE_KIND, 3).
-define(RED_ALREADY_DECIDED_KIND, 4).

%% Red heartbeats
-define(RED_HB_KIND, 5).
-define(RED_HB_ACK_KIND, 6).

%% Red delivery / abort
-define(RED_DELIVER_KIND, 7).
-define(RED_LEARN_ABORT_KIND, 8).

%% Control Plane Messages
-define(DC_PING, 10).
-define(DC_CREATE, 11).
-define(DC_GET_DESCRIPTOR, 12).
-define(DC_CONNECT_TO_DESCR, 13).
-define(DC_START_PAXOS_FOLLOWER, 14).

-record(red_prepare, {
    coord_location :: term(),
    tx_id :: term(),
    tx_label :: binary(),
    readset :: #{non_neg_integer() => [term()]},
    writeset :: #{non_neg_integer() => #{}},
    snapshot_vc :: vclock()
}).

-record(red_accept, {
    coord_location :: term(),
    ballot :: ballot(),
    tx_id :: term(),
    tx_label :: binary(),
    readset :: #{non_neg_integer() => [term()]},
    writeset :: #{non_neg_integer() => #{}},
    decision :: term(),
    prepare_vc :: vclock(),
    sequence_number :: non_neg_integer()
}).

-record(red_accept_ack, {
    ballot :: ballot(),
    tx_id :: term(),
    decision :: term(),
    prepare_vc :: vclock()
}).

-record(red_decision, {
    ballot :: ballot(),
    tx_id :: term(),
    decision :: term(),
    commit_ts :: non_neg_integer()
}).

-record(red_learn_abort, {
    ballot :: ballot(),
    tx_id :: term(),
    reason :: term(),
    commit_ts :: non_neg_integer()
}).

-record(red_already_decided, {
    tx_id :: term(),
    decision :: term(),
    commit_vc :: vclock()
}).

-record(red_heartbeat, {
    ballot :: ballot(),
    heartbeat_id :: term(),
    timestamp :: grb_time:ts(),
    sequence_number :: non_neg_integer()
}).

-record(red_heartbeat_ack, {
    ballot :: ballot(),
    heartbeat_id :: term(),
    timestamp :: grb_time:ts()
}).

-record(red_deliver, {
    ballot :: ballot(),
    sequence_number :: non_neg_integer(),
    timestamp :: grb_time:ts(),
    transactions :: [ { TxId :: term(), Label :: term() }
                    | { HB :: term(), HBId :: non_neg_integer() } ]
}).

-type replica_message() :: #red_prepare{}
                         | #red_accept{}
                         | #red_accept_ack{}
                         | #red_decision{}
                         | #red_already_decided{}
                         | #red_heartbeat{}
                         | #red_heartbeat_ack{}
                         | #red_deliver{}
                         | #red_learn_abort{}.

-export_type([replica_message/0]).
