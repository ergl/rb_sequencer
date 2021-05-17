-module(rb_sequencer_dc_utils).
-include("rb_sequencer.hrl").
-include_lib("kernel/include/logger.hrl").

-export([inter_dc_ip_port/0]).

%% Managing ETS tables
-export([safe_bin_to_atom/1]).

%% Starting timers
-export([maybe_send_after/2]).

%% Called via `erpc` or for debug purposes
-ignore_xref([inter_dc_ip_port/0]).

-define(BUCKET, <<"grb">>).
-define(header(Packet, Size), (Size):(Packet)/unit:8-integer-big-unsigned).

-spec inter_dc_ip_port() -> {inet:ip_address(), inet:port_number()}.
inter_dc_ip_port() ->
    {ok, IPString} = application:get_env(rb_sequencer, inter_dc_ip),
    {ok, IP} = inet:parse_address(IPString),
    {ok, Port} = application:get_env(rb_sequencer, inter_dc_port),
    {IP, Port}.

-spec safe_bin_to_atom(binary()) -> atom().
safe_bin_to_atom(Bin) ->
    case catch binary_to_existing_atom(Bin, latin1) of
        {'EXIT', _} -> binary_to_atom(Bin, latin1);
        Atom -> Atom
    end.

-spec maybe_send_after(non_neg_integer(), term()) -> reference() | undefined.
maybe_send_after(0, _) ->
    undefined;
maybe_send_after(Time, Msg) ->
    erlang:send_after(Time, self(), Msg).
