%%%-------------------------------------------------------------------
%%% @author pei
%%% @copyright (C) 2019, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 24. 六月 2019 11:22
%%%-------------------------------------------------------------------
-module(gz_nodes_init).
-author("pei").

%% API
-export([remote_change_config/1, init/0]).

-define(APP_NAME, gz_nodes).

eg_rebar_app_config()->
  [
    { gz_nodes, [
      {wait_time, 6000}
      ,{node_type, disc_copies}
      ,{main_nodes, ['chat_190116@192.168.133.55', 'chat_190116@192.168.133.56']}
      ,{dbs, [
        {db_sequence, [ {erlang_sequence, disc_copies} ]}
        ,{db_user, [ {user_online, disc_copies}, {user_device_uid, disc_copies}, {user_wx_cache, ram_copies}, {user_cache, ram_copies} ]}
        ,{db_token, [ {user_token, disc_copies}]}
        ,{db_friends, [{friend_cache, ram_copies}]}
        ,{db_server, [{server_instance, disc_copies}, {server_instance_info, disc_copies}]}
        ,{db_sys, [{sys_cache, ram_copies}]}
        ,{db_location, [{share_location, ram_copies}]}
        ,{db_friends, [{friend_cache, ram_copies}]}
      ]} ]
    }
  ].

init()->
%%  PgApp = proplists:get_value(pgapp, Opts, []),
%%  PgAppDefOpts = proplists:get_value(default, PgApp, []),
%%  pgapp:connect(PgAppDefOpts),

  Dbs = gz_nodes_util:get_env(?APP_NAME, dbs, []),
  NodeType = gz_nodes_util:get_env(?APP_NAME, node_type, disc_copies),
  MainNodes0 = gz_nodes_util:get_env(?APP_NAME, main_nodes, []),
  Opts = [{dbs, Dbs}],


  start(NodeType, MainNodes0, Opts).

%%%-------------------------------------------------------------------
start(disc_copies, [], Opts)->
  % see root and copy start
  lager:info("Start as root node"),
  mnesia:stop(),
  ok = mnesia:start(),

  ok = case mnesia:system_info(use_dir) of
         true -> ok;
         _ ->
           {atomic, ok} = mnesia:change_table_copy_type(schema, node(), disc_copies),
           ok
       end,
  % create tables
  ok = create_copy_tables(disc_copies, Opts);

start(disc_copies, Nodes0, Opts)->
  % see root and copy start
  ThisNode = node(),
  Nodes = gz_nodes_util:difference(Nodes0, [ThisNode]),

  ok = ensure_contact(Nodes),
  OnlineNodes = nodes(),
%%  OnlineMaster = gz_nodes_util:cross(Nodes, OnlineNodes),
  case OnlineNodes of
    []->
      lager:info("No OnlineNodes? ~p, ~p~n", [Nodes, OnlineNodes]),
      case lists:member(ThisNode, Nodes0) of
        true->start(disc_copies, [], Opts);
        _->
          LocalDbNodes = mnesia:system_info(db_nodes),
          lager:info("Not member? ~p~n", [[ThisNode, Nodes0, LocalDbNodes]]),
          case lists:any(fun(V)->lists:member(V, Nodes0) end, LocalDbNodes) of
            true->
              lager:info("Start as normal lonely node, AND dbnodes: ~p", [LocalDbNodes]),
              ok = create_copy_tables(disc_copies, Opts)
          end
      end;
    _ ->
      mnesia:stop(),
      ok = mnesia:start(),
      OnlineMaster = gz_nodes_util:cross(Nodes, OnlineNodes),
      LocalDbNodes = mnesia:system_info(db_nodes),
      lager:info("Start cluster node:Self:~p, Masters:~p, Nodes:~p, DBNodes:~p~n", [self(), OnlineMaster, OnlineNodes, LocalDbNodes]),
      ok = case gz_nodes_util:cross(OnlineMaster, LocalDbNodes) of
             [] ->
               Tables = mnesia:system_info(tables),
               case Tables of
                 [schema] ->
                   ThisNode = node(),
                   mnesia:stop(),
                   mnesia:delete_schema(ThisNode),
                   mnesia:start(),
                   ok = change_config(OnlineMaster),
                   case mnesia:change_table_copy_type(schema, ThisNode, disc_copies) of
                     {atomic, ok} ->
                       ok;
                     {aborted,{already_exists,schema,ThisNode,disc_copies}} ->
                       ok;
                     Err -> Err
                   end;
                 Err ->
                   lager:info("So, we may do so..."),
                   case mnesia:system_info(use_dir) of
                     true -> ok;
                     _ ->
                       {atomic, ok} = mnesia:change_table_copy_type(schema, node(), disc_copies),
                       ok
                   end
               end;
             _ ->
               % check schema is ok
               lager:info("So, we may do so..."),
               case mnesia:system_info(use_dir) of
                 true -> ok;
                 _ ->
                   {atomic, ok} = mnesia:change_table_copy_type(schema, node(), disc_copies),
                   ok
               end
           end,
      ok = mnesia:wait_for_tables([schema], infinity),
      ok = create_copy_tables(disc_copies, Opts)
  end;
start(ram_copies, [_|_]=Nodes, Opts)->
  % see normal start
  mnesia:stop(),
  false = mnesia:system_info(use_dir),
  ok = ensure_contact(Nodes),
  % start
  ok = mnesia:start(),
  OnlineNodes = nodes(),
  OnlineMaster = [X||X<-Nodes,Y<-OnlineNodes,X=:=Y],
  ok = change_config(OnlineMaster),

  ok = mnesia:wait_for_tables([schema], infinity),

  ok = create_copy_tables(ram_copies, Opts);
start(ram_copies, [], Opts)->
  % see root and copy start
  lager:info("Start as mem root node"),
  mnesia:stop(),
  ok = mnesia:start(),

  ok = case mnesia:system_info(use_dir) of
         true -> error;
         _ -> ok
       end,
  % create tables
  ok = create_copy_tables(disc_copies, Opts).


create_copy_tables(ST, Opts)->
  Mdbs = proplists:get_value(dbs, Opts, []),
  true = lists:all(
    fun
      ({Mod, DBS})->
        ok =:= node_create_copy_tables(Mod, ST, DBS);
      (P)->
        lager:warning("create_copy_tables: unkown db config", [P]),
        true
    end, Mdbs),
  ok.
%%  ok.

change_config([])->
  error;
change_config([Node|Nodes]) ->
  case rpc:call(Node, ?MODULE, remote_change_config, [node()]) of
    ok->ok;
    Err ->
      lager:info("[change_config]~p~n", [Err]),
      change_config(Nodes)
  end.

remote_change_config(Node)->
  Res = mnesia:change_config(extra_db_nodes, [Node]),
  lager:info("[remote_change_config]~w, ~w~n", [Node, Res]),
  case Res of
    ok -> ok;
    {ok, _} -> ok;
    Err -> Err
  end.

%%%-------------------------------------------------------------------
ensure_contact(ContactNodes0) ->
  ContactNodes = gz_nodes_util:difference(ContactNodes0, [node()]),
  Answering = [N||N<-ContactNodes, net_adm:ping(N)=:=pong],
  lager:info("ContactNodes: ~p, Answering: ~p~n", [ContactNodes, Answering]),
  case Answering of
    [] ->
      lager:warning("node not found: ~p~n", [ContactNodes]),
      ok;
%%      {error, no_contact_nodes_reacheable};
    _ ->
      DefaultTime = 6000,
      WaitTime = gz_nodes_util:get_env(?APP_NAME, wait_time, DefaultTime),
      wait_for_nodes(length(Answering), WaitTime)
  end.

wait_for_nodes(0, _WaitTime) ->
  error;
wait_for_nodes(MinNodes, WaitTime) ->
  Slices = 10,
  SliceTime = round(WaitTime/Slices),
  wait_for_nodes(MinNodes, SliceTime, Slices).
wait_for_nodes(_MinNodes, _SliceTime, 0) ->
  ok;
wait_for_nodes(MinNodes, SliceTime, Iterations) ->
  case length(nodes()) >= MinNodes of
    true -> ok;
    false ->
      timer:sleep(SliceTime),
      wait_for_nodes(MinNodes, SliceTime, Iterations - 1)
  end.
%%%-------------------------------------------------------------------


node_create_copy_tables(Mod, ST, [A|Left])->
  ok = node_create_copy_tables(Mod, ST, A),
  ok = node_create_copy_tables(Mod, ST, Left);
node_create_copy_tables(Mod, ST, A) when is_atom(A), is_atom(ST)->
  ok = Mod:create_table(A, ST),
  ok = gz_nodes_mnesia_helper:copy_table(A, ST);
node_create_copy_tables(Mod, disc_copies, {A, ST}) when is_atom(A)->
  ok = Mod:create_table(A, ST),
  ok = gz_nodes_mnesia_helper:copy_table(A, ST);
node_create_copy_tables(Mod, ram_copies, {A, ST}) when is_atom(A)->
  case ST of
    ram_copies->
      ok;
    _->
      lager:warning("[node_create_copy_tables]only ram_copies: ~p, ~p, ~p~n", [Mod, A, ST])
  end,
  ok = Mod:create_table(A, ram_copies),
  ok = gz_nodes_mnesia_helper:copy_table(A, ram_copies);
node_create_copy_tables(Mod, ST, DB)->
  lager:info("[node_create_copy_tables]~p, ~p, ~p~n", [Mod, ST, DB]),
  ok.


%%%%%--------------------------------------------------------------------------------------------------
