-module(rb_sequencer_app).
-behaviour(application).
-include_lib("kernel/include/logger.hrl").

%% Application callbacks
-export([start/2, stop/1]).

%% ===================================================================
%% Application callbacks
%% ===================================================================

start(_StartType, _StartArgs) ->
    case rb_sequencer_sup:start_link() of
        {error, Reason} ->
            {error, Reason};

        {ok, Pid} ->
            ok = enable_debug_logs(),
            ok = rb_sequencer_tcp_server:start_server(),
            ok = rb_sequencer_dc_connection_receiver:start_service(),
            {ok, Pid}
    end.

-ifdef(debug_log).
-spec enable_debug_logs() -> ok.
enable_debug_logs() ->
    logger:add_handler(debug, logger_std_h, #{
        filters => [{debug, {fun logger_filters:level/2, {stop, neq, debug}}}],
        config => #{file => "log/debug.log"}
    }).
-else.
-spec enable_debug_logs() -> ok.
enable_debug_logs() -> ok.
-endif.

stop(_State) ->
    ok.
