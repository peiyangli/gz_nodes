%%%-------------------------------------------------------------------
%%% @author pei
%%% @copyright (C) 2019, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 24. 六月 2019 11:25
%%%-------------------------------------------------------------------
-module(gz_nodes_util).
-author("pei").

%% API
-export([get_env/2, difference/2, cross/2]).

get_env(AppName, Key) ->
  get_env(AppName, Key, undefined).
get_env(AppName, Key, Default) ->
  case application:get_env(AppName, Key) of
    undefined -> Default;
    {ok, Value} -> Value
  end.



difference(L1, L2) when is_list(L1) andalso is_list(L2)-> L1 -- L2;
difference(L1, L2) when is_list(L1)-> L1 -- [L2];
difference(_, _)-> [].


cross(L1,L2) ->
  [X||X<-L1,Y<-L2,X=:=Y].