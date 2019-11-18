%%%-------------------------------------------------------------------
%% @doc emqx_mysql_hook public API
%% @end
%%%-------------------------------------------------------------------

-module(emqx_mysql_hook_app).

-behaviour(application).
-emqx_plugin(?MODULE).

-define(APP, emqx_mysql_hook).
-export([start/2, stop/1]).

start(_StartType, _StartArgs) ->
    {ok, Sup} = emqx_mysql_hook_sup:start_link(),
    ?APP:register_metrics(),
    ?APP:load(),
    emqx_mysql_hook_cfg:register(),
    {ok, Sup}.

stop(_State) ->
    ok.

%% internal functions
