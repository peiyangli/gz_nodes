%%%-------------------------------------------------------------------
%%% @author pei
%%% @copyright (C) 2018, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 26. 七月 2018 9:50
%%%-------------------------------------------------------------------
-module(db_user).
-author("pei").

-define(SERVER, ?MODULE).
-export([user_info_cache/2, user_info_cache_error/2, user_info_cache/3, user_info_cache_error/3]).
-export([set_wx_cache/2, get_wx_cache/2, delete_wx_cache/1]).
-export([zone_phone/2, device_uid/1, device_uid_del/2]).
-export([user_online/3, user_update_online_info/3, user_offline/2, user_online_info/1, user_online_info/2, user_online_all/1, os_type/1, os_typeid/1, user_online_offline_info/1, is_ios/1, user_online_info_of_type/2, user_logout/2, user_offline_unsafe/3]).
-export([register_user/1, user_info/2, user_info/1, set_user_info/3, user_info_phone/1, user_info_unionid/1, register_user/3, get_password/1, user/2]).
-export([create_table/2]).

-export([user_update_online_mod/3, user_reload_online_mod/1, user_online_mod/2, user_reload_online_mod/2]).

-export([user_cache/1, user_cache_set/3, user_info_cache_clear/1]).
-include("dbs.hrl").
-include("dbu.hrl").

-define(Seconds_OneDay, 86400).

%%  {atomic, ok} = mnesia:create_table(friends, [{type, bag}, {attributes, record_info(fields, friends)}]).

%%%%%=================================================================================================
%%%%%=================================================================================================
create_table(user_device_uid, ST)->
  gz_nodes_mnesia_helper:create_table(user_device_uid, set, ST, record_info(fields, user_device_uid));
create_table(user_cache, ST)->
  gz_nodes_mnesia_helper:create_table(user_cache, set, ST, record_info(fields, user_cache));
create_table(user_wx_cache, ST)->
  gz_nodes_mnesia_helper:create_table(user_wx_cache, set, ST, record_info(fields, user_wx_cache));
create_table(user_online, ST)->
  gz_nodes_mnesia_helper:create_table(user_online, set, ST, record_info(fields, user_online)).

user_cache(Key)->
  mnesia:dirty_read(user_cache, Key).
user_cache_set(Key, Val, Tm)->
  mnesia:dirty_write(user_cache, #user_cache{uid = Key, info = Val, tm = Tm}).
user_info_cache_clear(Uid)->
  mnesia:dirty_delete(user_cache, Uid).
%%%%%=================================================================================================

%%%%%=================================================================================================

zone_phone(Zone, Phone) ->
  <<Zone/binary, "-", Phone/binary>>.
%%%%%=================================================================================================
% readwrite
% {{Name, Password, Gender}, {Phone, Zone}, {Ip, Os, Device, Version}}
% db_user:register_user(#user{name="tom", phone = "+86-18208112611", password = "123456", gender = 1, createtime = erlang:system_time(second), ip="10.10.1.10", os=1, device = "win10", version = "0.0.1", status = 1}),
register_user({Name, Password, Gender}, {Phone, Zone}, {Ip, Os, Device, Version})->
  db_user:register_user(#{?name=>Name, ?phone => zone_phone(Zone, Phone), ?password => Password, ?gender => Gender, ?createtime => dg_tm:second(), ?ip=>Ip, ?os=>Os, ?device => Device, ?version => Version, ?status => 0}).


register_user(User0) when is_map(User0)->
  case db_sequence:next_uid() of
    {ok, _ID, Uid}->
      Now = dg_tm:second(),
      User = User0#{?uid => Uid, ?createtime =>Now, ?mtm => Now}, %, ?ctm => Now
      {Query, Values} = dbhelper:insert(user, User),
      pgapp:with_transaction(
        fun(Conn)->
          case pgapp:equery(Conn, Query, Values) of
            {ok, 1}->
              {ok, 1} = pgapp:equery(Conn, "INSERT INTO user_wy (uid) VALUES ($1)", [Uid]),
              {ok, 1} = pgapp:equery(Conn, "INSERT INTO user_ex (uid) VALUES ($1)", [Uid]),
              {ok, User};
            Err->
              Err
          end
        end);
    Err->Err
  end.


%%% KeyF = phone, id, uid, unionid
%%set_user_info({KeyF, KeyV}, User, {Ip, Comment}) when is_map(User) ->
%%  Fields = maps:keys(User),
%%  case Fields of
%%    []->{ok, 1};
%%    _->
%%      Now =
%%        case User of
%%          #{?mtm:=Now0} when is_integer(Now0)-> Now0;
%%          _-> dg_tm:second()
%%        end,
%%      FieldVs = term_to_binary(User),
%%      {Query, Values} = dbhelper:update(user, User#{?mtm => Now}, {[dbhelper:to_iodata(KeyF), "=$1"], [KeyV]}),
%%      pgapp:with_transaction(
%%        fun(Conn)->
%%          {ok, 1} = pgapp:equery(Conn, Query, Values),
%%          db_log_modify_user:insert(Conn, {Uid, Now, Ip, <<"user">>, Fields, FieldVs, Comment})
%%        end),
%%      pgapp:equery(Query, Values)
%%  end;
set_user_info(Uid, User, {Ip, Comment}) when is_map(User) ->
  Fields = maps:keys(User),
  case Fields of
    []->{ok, 1};
    _->
      Now =
      case User of
        #{?mtm:=Now0} when is_integer(Now0)-> Now0;
        _-> dg_tm:second()
      end,
      FieldVs = term_to_binary(User),
      {Query, Values} = dbhelper:update(user, User#{?mtm => Now}, {"uid=$1", [Uid]}),
      pgapp:with_transaction(
        fun(Conn)->
          {ok, 1} = pgapp:equery(Conn, Query, Values),
          db_log_modify_user:insert(Conn, {Uid, Now, Ip, <<"user">>, Fields, FieldVs, Comment})
        end),
      pgapp:equery(Query, Values)
  end.



% read only
-type user_unique_fields() :: uid|phone|unionid|binary().
-spec user_info(Field::user_unique_fields(), Uid::integer())->any().
user_info(_, [])->
  {ok, []};
user_info(Field, [Value])->
  QR = pgapp:equery(["SELECT * FROM \"user\" WHERE \"", dbhelper:to_iodata(Field), "\"=$1"], [Value]),
  dbhelper:result(QR);
user_info(Field, Values)when is_list(Values)->
  QR = pgapp:equery(["SELECT * FROM \"user\" WHERE \"", dbhelper:to_iodata(Field), "\"=ANY($1)"], [Values]),
  dbhelper:result(QR);
user_info(Field, Value)->
  QR = pgapp:equery(["SELECT * FROM \"user\" WHERE \"", dbhelper:to_iodata(Field), "\"=$1"], [Value]),
  dbhelper:result(QR).


user_info(Uid)->
  user_info(?uid, Uid).
user_info_phone(Phone)->
  user_info(?phone, Phone).
user_info_unionid(ID)->
  user_info(?unionid, ID).


user({KeyF, KeyV}, Fields) when is_list(KeyV)->
  dbhelper:select(user, Fields, {[dbhelper:to_iodata(KeyF), "=ANY($1)"], [KeyV]});
user({KeyF, KeyV}, Fields)->
  dbhelper:select(user, Fields, {[dbhelper:to_iodata(KeyF), "=$1"], [KeyV]}).





user_info_cache(_, [], _)->
  {error, []};
user_info_cache(Uid, Fields, CacheTime)->
  {Code, Infos} = user_info_cache_error(Uid, CacheTime),
  case Code of
    ok->
      [dbhelper:maps_fields(Fields, Info)||Info<-Infos];
    _->
      {Code, []}
  end.


user_info_cache_error(_, [], _CacheTime)->
  {error, []};
user_info_cache_error(Uid, Fields, CacheTime)->
  {Code, Infos} = user_info_cache_error(Uid, CacheTime),
  {Code, [dbhelper:maps_fields(Fields, Info)||Info<-Infos]}.


user_info_cache(Uid, CacheTime)->
  user_info_cache_error(Uid, CacheTime).


% when you cannot read database but youcan read some of the cache
user_info_cache_error(Uid, CacheTime) when is_integer(Uid)->
  case mnesia:dirty_read({user_cache, Uid}) of
    [#user_cache{info = Info, tm = OldTm}]->
      Now = dg_tm:second(),
      case  OldTm + CacheTime < Now of
        true->
          case user_info(Uid) of
            {ok, []}->
              {ok, []};
            {ok, [Info2]} ->
              mnesia:dirty_write(user_cache, #user_cache{uid = Uid, info = Info2, tm=Now}),
              {ok, [Info2]};
            Err->
              {Err, []}
          end;
        _->
          {ok, [Info]}
      end;
    []->
      case user_info(Uid) of
        {ok, []}->
          {ok, []};
        {ok, [Info2]} ->
          Now = dg_tm:second(),
          mnesia:dirty_write(user_cache, #user_cache{uid = Uid, info = Info2, tm=Now}),
          {ok, [Info2]};
        Err->
          {Err, []}
      end
  end;
user_info_cache_error([Uid], CacheTime)->
  user_info_cache_error(Uid, CacheTime);
user_info_cache_error(Uids = [_|_], CacheTime)->
  Now = dg_tm:second(),
  Oldest = Now - CacheTime,
  {Caches, FUids} =
    lists:foldl(
      fun(Uid, {CacheUids, CacheFaileds})->
        case mnesia:dirty_read({user_cache, Uid}) of
          [#user_cache{info = Info, tm = Tm}] when Tm < Oldest->
            {[Info|CacheUids], CacheFaileds};
          _->
            {CacheUids, [Uid|CacheFaileds]}
        end
      end, {[], []}, Uids),
  case user_info(FUids) of
    {ok, Infos}->
      [mnesia:dirty_write(user_cache, #user_cache{uid = Uid, info = Info, tm = Now}) || Info = #{uid := Uid} <- Infos],
      {ok, lists:append(Caches, Infos)};
    Err->
      {Err, Caches}
  end;
user_info_cache_error([], _CacheTime)->
  {ok, []}.
%%%%%=================================================================================================
% if long time no online, we should clear the information in table user_online

is_ios(T)->
  os_type(T) =:= ios.

os_type(2)->ios;
os_type(1)->android;
os_type(<<"ios">>)->ios;
os_type(<<"android">>)->android;
os_type(ios)->ios;
os_type(android)->android;
os_type("ios")->ios;
os_type("android")->android;
os_type(_)->other.

os_typeid(2)->2;
os_typeid(1)->1;
os_typeid(<<"ios">>)->2;
os_typeid(<<"android">>)->1;
os_typeid(ios)->2;
os_typeid(android)->1;
os_typeid("ios")->2;
os_typeid("android")->1;
os_typeid(_)->3.

% Device:: ios, android, other
-define(DEF_TOKEN_TTL, 300). % 5 min
% {atomic, {ok, UOI}|{ok, UOI, OldUoi}} | Error
% Srv = db_server:get_instance()
% DeviceToken = {Firm, DeviceTokenValue}
user_online(Uid, Addr, {Os0, Device, DeviceToken, DeviceId})->
  {ok, Srv} = db_server:get_instance(),
  Os1 = os_type(Os0),
  Time = dg_tm:second(),
  Token0 = dg_util:binary_to_hex(erlang:md5([<<Uid:64>>, "Token", <<Time:64>>])),
  Pid0 = self(),
  UOI0 = #{?Foi_srv=>Srv, ?Foi_online=>1, ?Foi_pid=>Pid0, ?Foi_os=>Os1, ?Foi_itm => Time, ?Foi_addr => Addr, ?Foi_token=>Token0, ?Foi_deviceid=>DeviceId, ?Foi_device => Device, ?Foi_devicetoken => DeviceToken},
  mnesia:transaction(
    fun()->
      case mnesia:read(user_online, Uid) of
        []->
          ok = mnesia:write(#user_online{uid = Uid, login = [UOI0]}),
          ok = mnesia_set_device_uid(Uid, DeviceToken),
          {ok, UOI0};
        [#user_online{login = Logins}]->
          {Same, Other} = lists:partition(fun(#{?Foi_os := OS}) -> os_type(OS) =:= Os1 end, Logins),
          case Same of
            [OldUOI = #{?Foi_online := Online, ?Foi_token := Token1, ?Foi_deviceid := OldDeviceId}]->
              % online
              {NewToken, UserOld} =
              case {Online, OldDeviceId} of
                {1, DeviceId}->{Token1, false};
                {1, _}->{Token0, true};
                {0, DeviceId}->{Token1, false};
                {0, _}->{Token0, false}
              end,
              UOI = UOI0#{?Foi_token=>NewToken},
              ok = mnesia:write(#user_online{uid = Uid, login = [UOI|Other]}),
              ok = mnesia_set_device_uid(Uid, DeviceToken),
              case UserOld of
                true->{ok, UOI, OldUOI};
                _->{ok, UOI}
              end;
            _->
              ok = mnesia:write(#user_online{uid = Uid, login = [UOI0|Other]}),
              ok = mnesia_set_device_uid(Uid, DeviceToken),
              {ok, UOI0}
          end
      end
    end
  ).



% #{?Foi_devicetoken => NewValue}
user_update_online_info(Uid, Os0, FoiMap) when is_map(FoiMap)->
  Os1 = os_type(Os0),
  DeviceToken = maps:get(?Foi_devicetoken, FoiMap, undefined),
  mnesia:transaction(
    fun()->
      case mnesia:read(user_online, Uid) of
        []->
          {error, not_found};
        [#user_online{login = Logins}]->
          {Same, Other} = lists:partition(fun(#{?Foi_os := OS}) -> os_type(OS) =:= Os1 end, Logins),
          case Same of
            []->
              {error, not_found};
            [OldUOI = #{?Foi_devicetoken := OldDeviceToken}]->
              UOI = maps:merge(OldUOI, FoiMap),
              ok = mnesia:write(#user_online{uid = Uid, login = [UOI|Other]}),
              ok = mnesia_set_device_uid(Uid, DeviceToken)
          end
      end
    end
  ).

% atomic user offline and logout
%%user_logout(Uid, Os0)->
%%  Os1 = os_type(Os0),
%%  Pid0 = self(),
%%  mnesia:transaction(
%%    fun()->
%%      case mnesia:read(user_online, Uid) of
%%        []-> not_found;
%%        [UO =#user_online{login = Logins}]->
%%          {Same, Other} = lists:partition(fun(#{?Foi_os := OS}) -> os_type(OS) =:= os_type(Os0) end, Logins),
%%          case Same of
%%            []-> not_found;
%%            [#{?Foi_pid := Pid, ?Foi_devicetoken := DeviceToken}]->
%%              case Pid0=:=Pid of
%%                true->
%%                  case Other of
%%                    []->ok = mnesia:delete_object(UO);
%%                    _->ok = mnesia:write(#user_online{uid = Uid, login = Other})
%%                  end,
%%                  ok = mnesia:delete_object(#user_device_uid{device = {Os1, DeviceToken}, uid = Uid});
%%                _->
%%                  not_allow
%%              end
%%          end
%%      end
%%    end
%%  ).

user_logout(Uid, Os0)->
  Time = dg_tm:second(),
  Pid0 = self(),
  mnesia:transaction(
    fun()->
      case mnesia:read(user_online, Uid) of
        []-> not_found;
        [#user_online{login = Logins}]->
          {Same, Other} = lists:partition(fun(#{?Foi_os := OS}) -> os_type(OS) =:= os_type(Os0) end, Logins),
          case Same of
            []-> not_found;
            [UOI = #{?Foi_pid := Pid, ?Foi_os := OS, ?Foi_device := Device, ?Foi_itm := ITm}]->
              case Pid0=:=Pid of
                true->
                  % not like
                  ok = mnesia:write(#user_online{uid = Uid, login = [#{?Foi_os => OS, ?Foi_itm => ITm, ?Foi_device => Device, ?Foi_online => 0, ?Foi_pid => undefined, ?Foi_otm => Time}|Other]});
                _->
                  not_allow
              end
          end
      end
    end
  ).

% read user by device token
device_uid(DeviceToken={_,_})->
  case mnesia:dirty_read({user_device_uid, DeviceToken}) of
    [#user_device_uid{uid = Uid}]->[Uid];
    _->[]
  end;
device_uid(_)->[].

device_uid_del(DeviceToken={_,_}, Uid)->
  mnesia:dirty_delete_object(#user_device_uid{device = DeviceToken, uid = Uid});
device_uid_del(_,_)->ok.

mnesia_set_device_uid(Uid, {Firm, DeviceTokenValue} = DeviceToken) when is_integer(Firm) andalso Firm > 0 andalso is_binary(DeviceTokenValue)->
  % if the old
  case byte_size(DeviceTokenValue)>0 of
    true->
      ok = mnesia:write(#user_device_uid{device = DeviceToken, uid = Uid});
    _->ok
  end;
mnesia_set_device_uid(_, _)->
  ok.




% {atomic, ok|not_found|not_allow}|Error
% user offline, maybe not logout
user_offline(Uid, Os0)->
  Time = dg_tm:second(),
  Pid0 = self(),
  mnesia:transaction(
    fun()->
      case mnesia:read(user_online, Uid) of
        []-> not_found;
        [#user_online{login = Logins}]->
          {Same, Other} = lists:partition(fun(#{?Foi_os := OS}) -> os_type(OS) =:= os_type(Os0) end, Logins),
          case Same of
            []-> ok;
            [UOI = #{?Foi_pid := Pid}]->
              case Pid0=:=Pid of
                true->
                  % not like
                  ok = mnesia:write(#user_online{uid = Uid, login = [UOI#{?Foi_online => 0, ?Foi_pid => undefined, ?Foi_otm => Time}|Other]});
                _->
                  not_allow
              end
          end
      end
    end
  ).


% for node exit only
user_offline_unsafe(Pred, Uid, Time) when is_function(Pred, 1)->
  mnesia:transaction(
    fun()->
      case mnesia:read(user_online, Uid) of
        []-> not_found;
        [#user_online{login = Logins}]->
          {Same, Other} = lists:partition(Pred, Logins),
          case Same of
            []-> ok;
            _->
              LoginsNew = lists:foldl(fun(UOI, Acc)->[UOI#{?Foi_online => 0, ?Foi_pid => undefined, ?Foi_otm => Time}|Acc] end, Other, Same),
              ok = mnesia:write(#user_online{uid = Uid, login = LoginsNew})
          end
      end
    end
  ).

% only online
user_online_info(Uid)->
  case mnesia:dirty_read({user_online, Uid}) of
    []->[];
    [#user_online{login = Logins}]->
      {Online, _Offline} = lists:partition(
        fun(#{?Foi_online := 0}) -> false;
          (#{?Foi_srv := {Node, Ins}})->
            case db_server:get_instance(Node) of
              {ok, {_, Ins}}->true;
              {ok, _}->false;
              _->true
            end;
          (_)->true
        end, Logins),
      Online
  end.


user_online_offline_info(Uid)->
  case mnesia:dirty_read({user_online, Uid}) of
    []->{[], []};
    [#user_online{login = Logins}]->
      lists:partition(
        fun(#{?Foi_online := 0}) -> false;
          (#{?Foi_srv := {Node, Ins}})->
            case db_server:get_instance(Node) of
              {ok, {_, Ins}}->true;
              {ok, _}->false;
              _->true
            end;
          (_)->true
        end, Logins)
  end.

% first type of
user_online_info(Uid, Os0)->
  user_online_info_of_type(user_online_all(Uid), Os0).
%%  lists:search(fun(#user_online_info{os = Os})-> os_type(Os) =:= os_type(Os0)  end, user_online_all(Uid)).

user_online_info_of_type([], _)->
  [];
user_online_info_of_type(All, Os0)->
  case lists:search(fun(#{?Foi_os := Os})-> os_type(Os) =:= os_type(Os0)  end, All) of
    {value, Value}->[Value];
    _->[]
  end.

% all offline or online
user_online_all(Uid)->
  case mnesia:dirty_read({user_online, Uid}) of
    []->[];
    [#user_online{login = Logins}]->
      Logins
  end.

%%%%%=================================================================================================
%%%%%=================================================================================================
get_password(User)->
  QR = pgapp:equery(["SELECT password FROM \"user\" WHERE uid=$1"], [User]),
  case dbhelper:result(QR) of
    {ok, []}->
      <<"">>;
    {ok, [#{?password:=Pwd}]}->
      Pwd
  end.
%%%%%=================================================================================================

set_wx_cache(ID, Info)->
  mnesia:dirty_write(user_wx_cache, #user_wx_cache{id = ID, info = Info, tm = dg_tm:second()}).

delete_wx_cache(ID)->
  mnesia:dirty_delete({user_wx_cache, ID}).


get_wx_cache(ID, infinity)->
  case mnesia:dirty_read({user_wx_cache, ID}) of
    [#user_wx_cache{info = Info}]->{ok, Info};
    _->{error, not_found}
  end;
get_wx_cache(ID, DetaTime)->
  case mnesia:dirty_read({user_wx_cache, ID}) of
    [#user_wx_cache{info = Info, tm = Tm}]->
      Now = dg_tm:second(),
      if
        Tm + DetaTime < Now -> {error, expaired};
        true->{ok, Info}
      end;
    _->{error, not_found}
  end.


%%%%%=================================================================================================

% get_user_online_extra
% if you set, call this method
user_update_online_mod(Uid, Mod, Blacks) when is_integer(Mod) andalso is_list(Blacks)->
  % write to db first
  db_config:set_user_online_mod(Uid, Mod, Blacks),
  {ok, OnlineMod} = db_config:get_user_online_mod_fields(Mod, Blacks),
  mnesia:dirty_write(#user_cache{uid={om, Uid}, info=OnlineMod, tm=dg_tm:second()}),
  {ok, OnlineMod}.

% when you login or logout, you can reload this one
user_reload_online_mod(Uid)->
  % write to db first
  case db_config:get_user_online_mod(Uid) of
    {ok, OnlineMod}->
      mnesia:dirty_write(#user_cache{uid={om, Uid}, info=OnlineMod, tm=dg_tm:second()}),
      {ok, OnlineMod};
    Err->
      Err
  end.

user_reload_online_mod(Uid, Js)->
  % write to db first
  case db_config:get_user_online_mod_jsonMap(Js) of
    {ok, OnlineMod}->
      mnesia:dirty_write(#user_cache{uid={om, Uid}, info=OnlineMod, tm=dg_tm:second()}),
      {ok, OnlineMod};
    Err->
      Err
  end.

user_online_mod(Uid, true)->
  case user_reload_online_mod(Uid) of
    {ok, OM}->{ok, OM};
    Err->
      case mnesia:dirty_read(user_cache, {om, Uid}) of
        [#user_cache{info = #uc_onlinemod{} = Info}] ->{ok, Info};
        _->{error, Err}
      end
  end;
user_online_mod(Uid, Expair) when is_integer(Expair)->
  case mnesia:dirty_read(user_cache, {om, Uid}) of
    [#user_cache{info = Info = #uc_onlinemod{}, tm = Tm}] ->
      Now = dg_tm:second(),
      case Tm + Expair < Now of
        true->user_online_mod(Uid, true);
        _->{ok, Info}
      end;
    _->user_online_mod(Uid, true)
  end;
user_online_mod(Uid, _)->
  case mnesia:dirty_read(user_cache, {om, Uid}) of
    [#user_cache{info = Info = #uc_onlinemod{}}] ->
      {ok, Info};
    _->user_online_mod(Uid, true)
  end.

%%%%%=================================================================================================



%%%%%=================================================================================================