%%%-------------------------------------------------------------------
%%% @author pei
%%% @copyright (C) 2019, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 24. 六月 2019 11:24
%%%-------------------------------------------------------------------
-module(gz_nodes_mnesia_helper).
-author("pei").

%% API
-export([create_table/4, copy_table/2]).

%%%%%--------------------------------------------------------------------------------------------------
create_table(Name, Type, SType, Attr)->
  Tables = mnesia:system_info(tables),
  case lists:member(Name, Tables) of
    true->
      RunningNodes = mnesia:table_info(Name, all_nodes),
      ok = case lists:member(node(), RunningNodes) of
             true->
               % yes we are the owner of this table, change copy type
               case mnesia:table_info(Name, storage_type) of
                 SType -> ok;
                 _->
                   {atomic, ok} = mnesia:change_table_copy_type(Name, node(), SType),
%%          {aborted,{already_exists,Name,_, SType}}
                   ok
               end;
             _ ->
               % we cannot find the table local node, so copy if from other one
               {atomic, ok} = mnesia:add_table_copy(Name, node(), SType),
               ok
           end,
      mnesia:wait_for_tables([Name], infinity);
    _ ->
      {atomic, ok} = mnesia:create_table(Name, [{type, Type}, {SType, [node()]},{attributes, Attr}]),
      ok
  end.

copy_table(Name, SType)->
  RunningNodes = mnesia:table_info(Name, all_nodes),
  ok = case lists:member(node(), RunningNodes) of
         true->
           case mnesia:table_info(Name, storage_type) of
             SType -> ok;
             _->
               {atomic, ok} = mnesia:change_table_copy_type(Name, node(), SType),
               ok
           end;
         _ ->
           {atomic, ok} = mnesia:add_table_copy(Name, node(), SType),
           ok
       end,
  ok = mnesia:wait_for_tables([Name], infinity).
