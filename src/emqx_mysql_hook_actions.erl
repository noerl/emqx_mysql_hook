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

-export([ on_action_create_data_to_mysql/2
        ]).

%%------------------------------------------------------------------------------
%% Actions for mysql hook
%%------------------------------------------------------------------------------

-spec(on_resource_create(binary(), map()) -> map()).
on_resource_create(ResId, Conf = #{<<"host">> := Host, <<"port">> := Port, <<"db">> := DB, <<"user">> := User, <<"pwd">> := Pwd}) ->
    io:format("Conf:~p~n", [Conf]),
    MysqlOption = [
        {host, binary_to_list(Host)}, 
        {port, Port}, 
        {user, binary_to_list(User)},
        {password, binary_to_list(Pwd)},
        {database, binary_to_list(DB)}
    ],
    case mysql:start_link(MysqlOption) of
        {ok, Pid} ->
            Conf#{pid => Pid};
        {error, Reason} ->
            ?LOG(error, "Initiate Resource ~p failed, ResId: ~p, ~0p",
                [?RESOURCE_TYPE_MYSQLHOOK, ResId, Reason]),
            error({connect_failure, Reason})
    end.
    

-spec(on_get_resource_status(binary(), map()) -> map()).
on_get_resource_status(_ResId, #{pid := Pid}) ->
    #{is_alive => erlang:is_process_alive(Pid)}.

-spec(on_resource_destroy(binary(), map()) -> ok | {error, Reason::term()}).
on_resource_destroy(_ResId, _Params) ->
    ok.

%% An action that forwards publish messages to mysql.
-spec(on_action_create_data_to_mysql(Id::binary(), #{}) -> action_fun()).
on_action_create_data_to_mysql(_Id, #{pid := Pid}) ->
    fun(Selected, _Envs) ->
        #{id := MessageId, payload := Payload} = Selected,
        Data = jsx:decode(Payload),
        MeterList = proplists:get_value(<<"meter_measurement">>, Data),

        io:format("Id:~p, Payload:~p~n", [MessageId, Payload]),
        Sql = "INSERT INTO mqtt_data (`message_id`, `serial_no`, `voltage_a`, `voltage_b`, `voltage_c`, `current_a`, `current_b`, `current_c`, `zero_line`, `open_record`, `open_numebr`, `conc_mode`, `is_steal`) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        Fun = fun(Meter) ->
            SerialNo = proplists:get_value(<<"serial_No">>, Meter),
            VoltageA = proplists:get_value(<<"voltage_a">>, Meter),
            VoltageB = proplists:get_value(<<"voltage_b">>, Meter),
            VoltageC = proplists:get_value(<<"voltage_c">>, Meter),
            CurrentA = proplists:get_value(<<"current_a">>, Meter),
            CurrentB = proplists:get_value(<<"current_b">>, Meter),
            CurrentC = proplists:get_value(<<"current_c">>, Meter),
            ZeroLine = proplists:get_value(<<"zero_line">>, Meter),
            OpenRecord = proplists:get_value(<<"open_record">>, Meter),
            OpenNumebr = proplists:get_value(<<"open_numebr">>, Meter),
            ConcMode = proplists:get_value(<<"conc_mode">>, Meter),
            IsSteal = proplists:get_value(<<"is_steal">>, Meter),
            [MessageId, SerialNo, VoltageA, VoltageB, VoltageC, CurrentA, CurrentB, CurrentC, ZeroLine, OpenRecord, OpenNumebr, ConcMode, IsSteal]
        end,
        NewMeterList = lists:map(Fun, MeterList),
        mysql:query(Pid, Sql, NewMeterList)
    end.
    
    

%%------------------------------------------------------------------------------
%% Internal functions
%%------------------------------------------------------------------------------

