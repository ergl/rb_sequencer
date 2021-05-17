#!/usr/bin/env escript
%% -*- erlang -*-
%%! -smp enable -hidden -name connect_dcs@127.0.0.1 -setcookie grb_cookie

-mode(compile).

-export([main/1]).

-spec usage() -> no_return().
usage() ->
    Name = filename:basename(escript:script_name()),
    io:fwrite(
        standard_error,
        "Usage: ~s [-l region] 'region:host:port' ... 'region_n:host:port' ~n",
        [Name]
    ),
    halt(1).

main(Args) ->
    case parse_args(Args, [leader_region]) of
        {error, Reason} ->
            io:fwrite(standard_error, "Wrong option: reason ~p~n", [Reason]),
            usage(),
            halt(1);
        {ok, #{leader_region := LeaderRegion, rest := Nodes}} ->
            erlang:put(leader_region, LeaderRegion),
            prepare(validate(parse_node_list(Nodes)))
    end.

-spec parse_node_list([term()]) -> {ok, [{term(), term(), non_neg_integer()}]} | {error, term()}.
parse_node_list([]) ->
    {error, emtpy_node_list};
parse_node_list(TargetList) ->
    lists:foldl(
        fun
            (_, {error, Reason}) -> {error, Reason};
            (Str, {ok, Acc}) ->
                case string:split(Str, ":", all) of
                    [RegionStr, IpStr, PortStr] ->
                        {ok, [{list_to_atom(RegionStr), IpStr, list_to_integer(PortStr)} | Acc ] };
                    _ ->
                        {error, {bad_target, Str}}
                end
        end,
        {ok, []},
        TargetList
    ).

%% @doc Validate parsing, then proceed
-spec validate({ok, [{term(), term(), non_neg_integer()}]} | error | {error, term()}) -> {ok, [{term(), term(), non_neg_integer()}]} | no_return().
validate(error) ->
    usage();
validate({error, Reason}) ->
    io:fwrite(standard_error, "Validate error: ~p~n", [Reason]),
    usage();
validate({ok, Payload}) ->
    {ok, Payload}.

-spec prepare({ok, [{term(), term(), non_neg_integer()}]} | undefined) -> ok | no_return().
prepare({ok, Nodes}) ->
    prepare(erlang:get(leader_region), Nodes).

prepare(undefined, _) ->
    io:fwrite(standard_error, "Node list given, but no leader region~n", []),
    usage();

prepare(LeaderRegion, AllNodes) ->
    io:format("Starting clustering at leader region ~p of nodes ~120p~n", [LeaderRegion, AllNodes]),
    case do_connect(lists:keyfind(LeaderRegion, 1, AllNodes), AllNodes) of
        {error, Reason} ->
            io:fwrite(standard_error, "Error connecting clusters: ~p~n", [Reason]),
            halt(1);
        {ok, Descriptors} ->
            io:format("Joined clusters ~p~n", [Descriptors]),
            ok
    end.

-spec do_connect(atom(), [{atom(), inet:ip_address(), inet:port_number()}]) -> ok.
do_connect(false, _) ->
    io:fwrite(standard_error, "Leader region is not among nodes given~n", []),
    usage();

do_connect({_LeaderRegion, LeaderIp, LeaderPort}, AllNodes) ->
    {ok, Socket} = gen_tcp:connect(LeaderIp, LeaderPort, [binary, {active, false}, {nodelay, true}, {packet, 4}]),
    %% 11:8 is the code for DC_CREATE msg
    ok = gen_tcp:send(Socket, <<0:8, 11:8, (term_to_binary(AllNodes))/binary>>),
    Resp = case gen_tcp:recv(Socket, 0) of
        {ok, Bin} ->
            binary_to_term(Bin);
        {error, Reason} ->
            {error, Reason}
    end,
    ok = gen_tcp:close(Socket),
    Resp.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% getopt
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

parse_args([], _) ->
    {error, noargs};
parse_args(Args, Required) ->
    case parse_args_inner(Args, #{}) of
        {ok, Opts} -> required(Required, Opts);
        Err -> Err
    end.

parse_args_inner([], Acc) ->
    {ok, Acc};
parse_args_inner([[$- | Flag] | Args], Acc) ->
    case Flag of
        [$l] ->
            parse_flag(Flag, Args, fun(Arg) -> Acc#{leader_region => list_to_atom(Arg)} end);
        [$h] ->
            usage(),
            halt(0);
        _ ->
            {error, {badarg, Flag}}
    end;
parse_args_inner(Words, Acc) ->
    {ok, Acc#{rest => Words}}.

parse_flag(Flag, Args, Fun) ->
    case Args of
        [FlagArg | Rest] -> parse_args_inner(Rest, Fun(FlagArg));
        _ -> {error, {noarg, Flag}}
    end.

required(Required, Opts) ->
    Valid = lists:all(fun(F) -> maps:is_key(F, Opts) end, Required),
    case Valid of
        true ->
            case maps:get(use_public_ip, Opts, false) of
                false ->
                    {ok, Opts};
                true ->
                    if
                        is_map_key(inter_dc_port, Opts) ->
                            {ok, Opts};
                        true ->
                            {error, "IP addresses, but no id len of port specified"}
                    end
            end;
        false -> {error, "Missing required fields"}
    end.
