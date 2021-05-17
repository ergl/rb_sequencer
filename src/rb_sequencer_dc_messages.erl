-module(rb_sequencer_dc_messages).
-include("rb_sequencer.hrl").
-include("dc_messages.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.


-define(header(Packet, Size), (Size):(Packet)/unit:8-integer-big-unsigned).

%% Msg API
-export([ping/1]).

-export([red_heartbeat/4,
         red_heartbeat_ack/3]).

-export([red_prepare/6,
         red_accept/9,
         red_accept_ack/4,
         red_decision/4,
         red_already_decided/3,
         red_learn_abort/4,
         red_deliver/4]).

-export([decode_payload/1]).

-export([frame/1]).

-spec frame(iodata()) -> iodata().
frame(Data) ->
    Size = erlang:iolist_size(Data),
    [<<?header(?INTER_DC_SOCK_PACKET_OPT, Size)>>, Data].

-spec ping(replica_id()) -> binary().
ping(ReplicaId) ->
    Payload = term_to_binary(ReplicaId),
    <<?VERSION:?VERSION_BITS, ?DC_PING:?MSG_KIND_BITS, Payload/binary>>.

-spec red_heartbeat(non_neg_integer(), ballot(), term(), grb_time:ts()) -> binary().
red_heartbeat(Sequence, Ballot, Id, Time) ->
    encode_msg(#red_heartbeat{ballot=Ballot, heartbeat_id=Id, timestamp=Time, sequence_number=Sequence}).

-spec red_heartbeat_ack(ballot(), term(), grb_time:ts()) -> binary().
red_heartbeat_ack(Ballot, Id, Time) ->
    encode_msg(#red_heartbeat_ack{ballot=Ballot, heartbeat_id=Id, timestamp=Time}).

-spec red_learn_abort(ballot(), term(), term(), grb_vclock:ts()) -> binary().
red_learn_abort(Ballot, TxId, Reason, CommitTs) ->
    encode_msg(#red_learn_abort{ballot=Ballot, tx_id=TxId, reason=Reason, commit_ts=CommitTs}).

-spec red_deliver(Ballot :: ballot(),
                  Sequence :: non_neg_integer(),
                  Timestamp :: grb_time:ts(),
                  TransactionIds :: [ { term(), tx_label() } | red_heartbeat_id()]) -> binary().

red_deliver(Sequence, Ballot, Timestamp, TransactionIds) ->
    encode_msg(#red_deliver{ballot=Ballot, timestamp=Timestamp, sequence_number=Sequence, transactions=TransactionIds}).

-spec red_prepare(red_coord_location(), term(), tx_label(), partition_rs(), partition_ws(), vclock()) -> binary().
red_prepare(Coordinator, TxId, Label, RS, WS, SnapshotVC) ->
    encode_msg(#red_prepare{coord_location=Coordinator,
                            tx_id=TxId,
                            tx_label=Label,
                            readset=RS,
                            writeset=WS,
                            snapshot_vc=SnapshotVC}).

-spec red_accept(non_neg_integer(), red_coord_location(), ballot(), red_vote(), term(), tx_label(), partition_rs(), partition_ws(), vclock()) -> binary().
red_accept(Sequence, Coordinator, Ballot, Vote, TxId, Label, PRS, PWS, PrepareVC) ->
    encode_msg(#red_accept{coord_location=Coordinator, ballot=Ballot, decision=Vote, sequence_number=Sequence,
                           tx_id=TxId, tx_label=Label, readset=PRS, writeset=PWS, prepare_vc=PrepareVC}).

-spec red_accept_ack(ballot(), red_vote(), term(), vclock()) -> binary().
red_accept_ack(Ballot, Vote, TxId, PrepareVC) ->
    encode_msg(#red_accept_ack{ballot=Ballot, decision=Vote, tx_id=TxId, prepare_vc=PrepareVC}).

-spec red_decision(ballot(), red_vote(), term(), grb_time:ts()) -> binary().
red_decision(Ballot, Decision, TxId, CommiTs) ->
    encode_msg(#red_decision{ballot=Ballot, decision=Decision, tx_id=TxId, commit_ts=CommiTs}).

-spec red_already_decided(red_vote(), term(), vclock()) -> binary().
red_already_decided(Decision, TxId, CommitVC) ->
    encode_msg(#red_already_decided{decision=Decision, tx_id=TxId, commit_vc=CommitVC}).

-spec encode_msg(replica_message()) -> binary().
encode_msg(Payload) ->
    {MsgKind, Msg} = encode_payload(Payload),
    <<?VERSION:?VERSION_BITS, MsgKind:?MSG_KIND_BITS, Msg/binary>>.

encode_payload(#red_prepare{coord_location=Coordinator, tx_id=TxId, tx_label=Label, readset=RS, writeset=WS, snapshot_vc=VC}) ->
    {?RED_PREPARE_KIND, term_to_binary({Coordinator, TxId, Label, RS, WS, VC})};

encode_payload(#red_accept{coord_location=Coordinator, ballot=Ballot, tx_id=TxId, sequence_number=Seq,
                           tx_label=Label, readset=RS, writeset=WS, decision=Vote, prepare_vc=VC}) ->
    {?RED_ACCEPT_KIND, term_to_binary({Coordinator, Ballot, TxId, Label, RS, WS, Vote, VC, Seq})};

encode_payload(#red_accept_ack{ballot=Ballot, tx_id=TxId, decision=Vote, prepare_vc=PrepareVC}) ->
    {?RED_ACCEPT_ACK_KIND, term_to_binary({Ballot, TxId, Vote, PrepareVC})};

encode_payload(#red_decision{ballot=Ballot, tx_id=TxId, decision=Decision, commit_ts=CommitTs}) ->
    {?RED_DECIDE_KIND, term_to_binary({Ballot, TxId, Decision, CommitTs})};

encode_payload(#red_already_decided{tx_id=TxId, decision=Vote, commit_vc=CommitVC}) ->
    {?RED_ALREADY_DECIDED_KIND, term_to_binary({TxId, Vote, CommitVC})};

encode_payload(#red_learn_abort{ballot=B, tx_id=TxId, reason=Reason, commit_ts=CommitTs}) ->
    {?RED_LEARN_ABORT_KIND, term_to_binary({B, TxId, Reason, CommitTs})};

encode_payload(#red_deliver{ballot=Ballot, timestamp=Timestamp, sequence_number=Seq, transactions=TransactionIds}) ->
    {?RED_DELIVER_KIND, term_to_binary({Ballot, Timestamp, Seq, TransactionIds})};

%% red heartbeat payloads

encode_payload(#red_heartbeat{ballot=B, heartbeat_id=Id, timestamp=Ts, sequence_number=Seq}) ->
    {?RED_HB_KIND, term_to_binary({B, Id, Ts, Seq})};

encode_payload(#red_heartbeat_ack{ballot=B, heartbeat_id=Id, timestamp=Ts}) ->
    {?RED_HB_ACK_KIND, term_to_binary({B, Id, Ts})}.

-spec decode_payload(binary()) -> replica_message().
decode_payload(<<?RED_PREPARE_KIND:?MSG_KIND_BITS, Payload/binary>>) ->
    {Coordinator, Tx, Label, RS, WS, VC} = binary_to_term(Payload),
    #red_prepare{coord_location=Coordinator, tx_id=Tx, tx_label=Label, readset=RS, writeset=WS, snapshot_vc=VC};

decode_payload(<<?RED_ACCEPT_KIND:?MSG_KIND_BITS, Payload/binary>>) ->
    {Coordinator, Ballot, TxId, Label, RS, WS, Vote, VC, Seq} = binary_to_term(Payload),
    #red_accept{coord_location=Coordinator, ballot=Ballot, tx_id=TxId, sequence_number=Seq,
                tx_label=Label, readset=RS, writeset=WS, decision=Vote, prepare_vc=VC};

decode_payload(<<?RED_ACCEPT_ACK_KIND:?MSG_KIND_BITS, Payload/binary>>) ->
    {Ballot, TxId, Vote, PrepareVC} = binary_to_term(Payload),
    #red_accept_ack{ballot=Ballot, tx_id=TxId, decision=Vote, prepare_vc=PrepareVC};

decode_payload(<<?RED_DECIDE_KIND:?MSG_KIND_BITS, Payload/binary>>) ->
    {Ballot, TxId, Decision, CommitTs} = binary_to_term(Payload),
    #red_decision{ballot=Ballot, tx_id=TxId, decision=Decision, commit_ts=CommitTs};

decode_payload(<<?RED_ALREADY_DECIDED_KIND:?MSG_KIND_BITS, Payload/binary>>) ->
    {TxId, Vote, CommitVC} = binary_to_term(Payload),
    #red_already_decided{tx_id=TxId, decision=Vote, commit_vc=CommitVC};

decode_payload(<<?RED_LEARN_ABORT_KIND:?MSG_KIND_BITS, Payload/binary>>) ->
    {B, TxId, Reason, CommitTs} = binary_to_term(Payload),
    #red_learn_abort{ballot=B, tx_id=TxId, reason=Reason, commit_ts=CommitTs};

decode_payload(<<?RED_DELIVER_KIND:?MSG_KIND_BITS, Payload/binary>>) ->
    {Ballot, Timestamp, Seq, TransactionIds} = binary_to_term(Payload),
    #red_deliver{ballot=Ballot, timestamp=Timestamp, sequence_number=Seq, transactions=TransactionIds};

decode_payload(<<?RED_HB_KIND:?MSG_KIND_BITS, Payload/binary>>) ->
    {B, Id, Ts, Seq} = binary_to_term(Payload),
    #red_heartbeat{ballot=B, heartbeat_id=Id, timestamp=Ts, sequence_number=Seq};

decode_payload(<<?RED_HB_ACK_KIND:?MSG_KIND_BITS, Payload/binary>>) ->
    {B, Id, Ts} = binary_to_term(Payload),
    #red_heartbeat_ack{ballot=B, heartbeat_id=Id, timestamp=Ts}.

%% Util functions

-ifdef(TEST).
rb_sequencer_dc_messages_utils_test() ->
    ReplicaId = dc_id1,
    Coordinator = {coord, ReplicaId, node()},
    VC = #{ReplicaId => 10},

    Payloads = [
        #red_prepare{
            coord_location = Coordinator,
            tx_id = ignore,
            tx_label = <<>>,
            readset = [foo],
            writeset = #{foo => bar},
            snapshot_vc = VC
        },
        #red_accept{
            coord_location = Coordinator,
            tx_id = ignore,
            tx_label = <<>>,
            readset = [foo],
            writeset = #{foo => bar},
            decision = ok,
            prepare_vc = VC,
            sequence_number = 10
        },
        #red_accept_ack{ballot=0, tx_id=ignore, decision=ok, prepare_vc=VC},
        #red_decision{ballot=10, tx_id=ignore, decision=ok, commit_ts=grb_vclock:get_time(?RED_REPLICA, VC)},
        #red_already_decided{tx_id=ignore, decision=ok, commit_vc=VC},
        #red_learn_abort{ballot=10, tx_id=ignore, commit_ts=grb_vclock:get_time(?RED_REPLICA, VC)},
        #red_deliver{
            ballot=10,
            timestamp=10,
            sequence_number=10,
            transactions=[ {?red_heartbeat_marker, 0}, {tx_0, <<"foo">>}]
        },

        #red_heartbeat{ballot=4, heartbeat_id={?red_heartbeat_marker, 0}, timestamp=10, sequence_number=10},
        #red_heartbeat_ack{ballot=4, heartbeat_id={?red_heartbeat_marker, 0}, timestamp=10}
    ],

    lists:foreach(fun(Msg) ->
        Bin = encode_msg(Msg),
        << ?VERSION:?VERSION_BITS, BinPayload/binary >> = Bin,
        ?assertEqual(Msg, decode_payload(BinPayload))
    end, Payloads).

-endif.
