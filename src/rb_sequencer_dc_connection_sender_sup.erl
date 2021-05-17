-module(rb_sequencer_dc_connection_sender_sup).
-behavior(supervisor).
-include("rb_sequencer.hrl").

-export([start_link/0,
         start_connection/3,
         start_connection_child/3]).

-export([init/1]).

-type handle() :: rb_sequencer_dc_connection_sender:t().
-export_type([handle/0]).

-ignore_xref([start_link/0]).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

-spec start_connection(TargetReplica :: replica_id(),
                       Ip :: inet:ip_address(),
                       Port :: inet:port_number()) -> {ok, [ handle(), ... ]}.

start_connection(TargetReplica, Ip, Port) ->
    ConnNum = rb_sequencer_dc_connection_manager:sender_pool_size(),
    start_connection(TargetReplica, Ip, Port, ConnNum, []).

-spec start_connection(TargetReplica :: replica_id(),
                       Ip :: inet:ip_address(),
                       Port :: inet:port_number(),
                       N :: non_neg_integer(),
                       Acc :: [ handle() ]) -> {ok, [ handle(), ... ]}.

start_connection(_, _, _, 0, Acc) ->
    {ok, Acc};
start_connection(TargetReplica, Ip, Port, N, Acc) ->
    {ok, Handle} = rb_sequencer_dc_connection_sender:start_connection(TargetReplica, Ip, Port),
    start_connection(TargetReplica, Ip, Port, N - 1, [Handle | Acc]).

start_connection_child(TargetReplica, Ip, Port) ->
    supervisor:start_child(?MODULE, [TargetReplica, Ip, Port]).


init([]) ->
    {ok, {{simple_one_for_one, 5, 10},
        [{rb_sequencer_dc_connection_sender,
            {rb_sequencer_dc_connection_sender, start_link, []},
            transient, 5000, worker, [rb_sequencer_dc_connection_sender]}]
    }}.
