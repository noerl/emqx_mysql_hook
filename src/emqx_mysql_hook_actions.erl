-module(emqx_mysql_hook_actions).

-include_lib("emqx/include/emqx.hrl").
-include_lib("emqx/include/logger.hrl").

-define(RESOURCE_TYPE_MYSQLHOOK, 'mysql_hook').
-define(RESOURCE_CONFIG_SPEC, #{
            host => #{type => string,
                     required => true,
                     order => 1,
                     default => <<"127.0.0.1">>,
                     title => #{en => <<"HOST">>,
                                zh => <<"HOST"/utf8>>},
                     description => #{en => <<"HOST">>,
                                      zh => <<"HOST"/utf8>>}},
            port => #{type => number,
                     default => 3306,
                     order => 2,
                     title => #{en => <<"PORT">>,
                                zh => <<"端口"/utf8>>},
                     description => #{en => <<"PORT">>,
                                      zh => <<"端口"/utf8>>}},
            db => #{type => string,
                     default => <<"">>,
                     order => 5,
                     title => #{en => <<"Database">>,
                                zh => <<"数据库"/utf8>>},
                     description => #{en => <<"Database">>,
                                      zh => <<"数据库"/utf8>>}},
            user => #{type => string,
                     default => <<"root">>,
                     order => 3,
                     title => #{en => <<"UserName">>,
                                zh => <<"用户名"/utf8>>},
                     description => #{en => <<"UserName">>,
                                      zh => <<"用户名"/utf8>>}},
            pwd => #{type => string,
                     default => <<"">>,
                     order => 4,
                     title => #{en => <<"Password">>,
                                zh => <<"密码"/utf8>>},
                     description => #{en => <<"Password">>,
                                      zh => <<"密码"/utf8>>}}
        }).

-define(ACTION_PARAM_RESOURCE, #{
            type => string,
            required => true,
            title => #{en => <<"Resource ID">>,
                       zh => <<"资源 ID"/utf8>>},
            description => #{en => <<"Bind a resource to this action">>,
                             zh => <<"给动作绑定一个资源"/utf8>>}
        }).

-define(ACTION_DATA_SPEC, #{
            '$resource' => ?ACTION_PARAM_RESOURCE
        }).

-define(JSON_REQ(URL, HEADERS, BODY), {(URL), (HEADERS), "application/json", (BODY)}).

-resource_type(#{name => ?RESOURCE_TYPE_MYSQLHOOK,
                 create => on_resource_create,
                 status => on_get_resource_status,
                 destroy => on_resource_destroy,
                 params => ?RESOURCE_CONFIG_SPEC,
                 title => #{en => <<"MysqlHook">>,
                            zh => <<"MysqlHook"/utf8>>},
                 description => #{en => <<"MysqlHook">>,
                                  zh => <<"MysqlHook"/utf8>>}
                }).

-rule_action(#{name => data_to_mysql,
               for => '$any',
               create => on_action_create_data_to_mysql,
               params => ?ACTION_DATA_SPEC,
               types => [?RESOURCE_TYPE_MYSQLHOOK],
               title => #{en => <<"Data to Mysql">>,
                          zh => <<"写数据到 Mysql 服务"/utf8>>},
               description => #{en => <<"Write Messages to Mysql">>,
                                zh => <<"写数据到 Mysql 服务"/utf8>>}
              }).

-type(action_fun() :: fun((Data :: map(), Envs :: map()) -> Result :: any())).


-export_type([action_fun/0]).

-export([ on_resource_create/2
        , on_get_resource_status/2
        , on_resource_destroy/2
        ]).

%% Callbacks of ecpool Worker
-export([connect/1]).

-export([ on_action_create_data_to_mysql/2
        ]).

%%------------------------------------------------------------------------------
%% Actions for mysql hook
%%------------------------------------------------------------------------------

-spec(on_resource_create(binary(), map()) -> map()).
on_resource_create(ResId, Conf = #{<<"host">> := Host, <<"port">> := Port, <<"db">> := DB, <<"user">> := User, <<"pwd">> := Pwd}) ->
    {ok, _} = application:ensure_all_started(ecpool),
    io:format("Conf:~p~n", [Conf]),
    PoolName = list_to_atom("mysql:" ++ binary_to_list(ResId)),
    Options = [
        {pool_size, 2},
        {pool_name, PoolName},
        {host, binary_to_list(Host)}, 
        {port, Port}, 
        {user, binary_to_list(User)},
        {password, binary_to_list(Pwd)},
        {database, binary_to_list(DB)}
    ],
    start_resource(ResId, PoolName, Options),
    case test_resource_status(PoolName) of
        true -> ok;
        false ->
            on_resource_destroy(ResId, #{<<"pool">> => PoolName}),
            error({{?RESOURCE_TYPE_MYSQLHOOK, ResId}, connection_failed})
    end,
    #{<<"pool">> => PoolName}.

    
    

-spec(on_get_resource_status(binary(), map()) -> map()).
on_get_resource_status(_ResId, #{<<"pool">> := PoolName}) ->
    IsAlive = test_resource_status(PoolName),
    #{is_alive => IsAlive}.


-spec(on_resource_destroy(binary(), map()) -> ok | {error, Reason::term()}).
on_resource_destroy(ResId, #{<<"pool">> := PoolName}) ->
    ?LOG(info, "Destroying Resource ~p, ResId: ~p", [?RESOURCE_TYPE_MYSQLHOOK, ResId]),
    case ecpool:stop_sup_pool(PoolName) of
        ok ->
            ?LOG(info, "Destroyed Resource ~p Successfully, ResId: ~p", [?RESOURCE_TYPE_MYSQLHOOK, ResId]);
        {error, Reason} ->
            ?LOG(error, "Destroy Resource ~p failed, ResId: ~p, ~p", [?RESOURCE_TYPE_MYSQLHOOK, ResId, Reason]),
            error({{?RESOURCE_TYPE_MYSQLHOOK, ResId}, destroy_failed})
    end.

%% An action that forwards publish messages to mysql.
-spec(on_action_create_data_to_mysql(Id::binary(), #{}) -> action_fun()).
on_action_create_data_to_mysql(_Id, #{<<"pool">> := PoolName}) ->
    fun(Selected, _Envs) ->
        #{id := MessageId, payload := Payload} = Selected,
        io:format("Selected:~p~n_Envs:~p~n", [Selected, _Envs]),
        io:format("Id:~p, Payload:~p~n", [MessageId, Payload]),

        Data = jsx:decode(Payload),
        MeterList = proplists:get_value(<<"meter_measurement">>, Data),

        Sql = "INSERT INTO mqtt_data (`message_id`, `serial_no`, `voltage_a`, `voltage_b`, `voltage_c`, `current_a`, `current_b`, `current_c`, `zero_line`, `open_record`, `open_numebr`, `conc_mode`, `is_steal`) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        ecpool:with_client(PoolName, 
            fun(Pid) -> 
                mysql:prepare(Pid, insert_mqtt_data, Sql)
            end
        ),
        ecpool:with_client(PoolName, 
            fun(Pid) -> 
                Fun = fun(Meter) ->
                    SerialNo = proplists:get_value(<<"serial_No">>, Meter),
                    VoltageA = mysql_value(proplists:get_value(<<"voltage_a">>, Meter)),
                    VoltageB = mysql_value(proplists:get_value(<<"voltage_b">>, Meter)),
                    VoltageC = mysql_value(proplists:get_value(<<"voltage_c">>, Meter)),
                    CurrentA = mysql_value(proplists:get_value(<<"current_a">>, Meter)),
                    CurrentB = mysql_value(proplists:get_value(<<"current_b">>, Meter)),
                    CurrentC = mysql_value(proplists:get_value(<<"current_c">>, Meter)),
                    ZeroLine = mysql_value(proplists:get_value(<<"zero_line">>, Meter)),
                    OpenRecord = mysql_value(proplists:get_value(<<"open_record">>, Meter)),
                    OpenNumebr = mysql_value(proplists:get_value(<<"open_numebr">>, Meter)),
                    ConcMode = mysql_value(proplists:get_value(<<"conc_mode">>, Meter)),
                    IsSteal = mysql_value(proplists:get_value(<<"is_steal">>, Meter)),
                    MeterData = [MessageId, SerialNo, VoltageA, VoltageB, VoltageC, CurrentA, CurrentB, CurrentC, ZeroLine, OpenRecord, OpenNumebr, ConcMode, IsSteal],
                    Result = mysql:execute(Pid, insert_mqtt_data, MeterData),
                    io:format("Result:~p~n", [Result])
                end,
                lists:foreach(Fun, MeterList)
            end
        )
    end.


connect(Options) ->
    mysql:start_link(Options).
    
    
start_resource(ResId, PoolName, Options) ->
    case ecpool:start_sup_pool(PoolName, ?MODULE, Options) of
        {ok, _} ->
            ?LOG(info, "Initiated Resource ~p Successfully, ResId: ~p", [?RESOURCE_TYPE_MYSQLHOOK, ResId]);
        {error, {already_started, _Pid}} ->
            on_resource_destroy(ResId, #{<<"pool">> => PoolName}),
            start_resource(ResId, PoolName, Options);
        {error, Reason} ->
            ?LOG(error, "Initiate Resource ~p failed, ResId: ~p, ~p", [?RESOURCE_TYPE_MYSQLHOOK, ResId, Reason]),
            on_resource_destroy(ResId, #{<<"pool">> => PoolName}),
            error({{?RESOURCE_TYPE_MYSQLHOOK, ResId}, create_failed})
    end.

test_resource_status(PoolName) ->
    Status = [erlang:is_process_alive(Worker) || {_WorkerName, Worker} <- ecpool:workers(PoolName)],
    lists:any(fun(St) -> St =:= true end, Status).


mysql_value(<<>>) -> null;
mysql_value(Value) -> Value.
%%------------------------------------------------------------------------------
%% Internal functions
%%------------------------------------------------------------------------------

