%% @doc
%%   native api to distributed actors
-module(ambitz).
-include("ambitz.hrl").
-include("include/ambitz.hrl").

-export([behaviour_info/1]).
-export([start/0]).
-export([
   start_link/1,
   call/3,
   call/4
]).
-export([
   entity/1
  ,entity/2
  ,service/1
  ,service/2
]).
-export([
   spawn/1,
   spawn/2,
   lookup/1,
   lookup/2,
   free/1,
   free/2
]).

-type(key()    :: binary()).
-type(entity() :: #entity{}).

%%%----------------------------------------------------------------------------   
%%%
%%% request behavior
%%%
%%%----------------------------------------------------------------------------   

%%
%% 
behaviour_info(callbacks) ->
   [
      %%
      %% ensure presence of actor in the cluster
      %%
      %% -spec(ensure/3 :: ([ek:vnode()], key(), opts()) -> false | [ek:vnode()]).
      {ensure,   3}

      %%
      %% generate globally unique transaction id
      %% -spec(guid/1 :: (any()) -> any()).
     ,{guid,     1}

      %%
      %% monitor transaction actor
      %%
      %% -spec(monitor/1 :: (ek:vnode()) -> reference()). 
     ,{monitor, 1}

      %%
      %% asynchronously cast request to transaction actor
      %%
      %% -spec(cast/4 :: (ek:vnode(), key(), req(), opts()) -> reference()). 
     ,{cast,    4} 

      %%
      %% accept response from transaction actor, 
      %% returns value and its signature
      %%
      %% -spec(unit/1 :: (any()) -> {any(), any()}).
     ,{unit,    1}

      %%
      %% accumulates and merges correlated response
      %%
      %% -spec(join/2 :: (any(), any()) -> any()).
     ,{join,    2}

      %%
      %% optional - read repair
      %% 
      %% -spec(repair/2 :: (peers(), any()) -> ok).
      %% {repair, 2}
   ];
behaviour_info(_) ->
   undefined.

%%%----------------------------------------------------------------------------   
%%%
%%% request coordinator interface
%%%
%%%----------------------------------------------------------------------------   

%%
%% RnD application start
start() ->
   applib:boot(?MODULE, code:where_is_file("app.config")).

%%
%% start request coordinator process
-spec(start_link/1 :: (atom()) -> {ok, pid()} | {error, any()}).

start_link(Mod) ->
   ambitz_req:start_link(Mod).   

%%
%% request distributed actor
-spec(call/3 :: (atom(), binary(), any()) -> any() | {error, any()}).
-spec(call/4 :: (atom(), binary(), any(), list()) -> any() | {error, any()}).

call(Pool, Key, Req) ->
   ambitz_req:call(Pool, Key, Req, []).
   
call(Pool, Key, Req, Opts) ->
   ambitz_req:call(Pool, Key, Req, Opts).

%%%----------------------------------------------------------------------------   
%%%
%%% api
%%%
%%%----------------------------------------------------------------------------   

%%
%% create casual context for ambit entity
-spec(entity/1 :: (binary()) -> entity()).
-spec(entity/2 :: (binary(), any()) -> entity()).

entity(Key) ->
   #entity{key = Key}.

entity(Key, Service) ->
   #entity{key = Key, val = Service}.


%%
%% get casual context property
-spec(service/1 :: (entity()) -> any() | undefined).

service(#entity{val = Service}) ->
   Service.

%%
%% get casual context property
-spec(service/2 :: (entity(), any()) -> entity()).

service(#entity{} = Ent, Service) ->
   Ent#entity{val = Service}.

%%
%% spawn service on the cluster
%%  Options
%%    w - number of succeeded writes
-spec(spawn/1 :: (entity()) -> entity() | {error, any()}).
-spec(spawn/2 :: (entity(), list()) -> entity() | {error, any()}).

spawn(Entity) ->
   ambitz:spawn(Entity, []).

spawn(#entity{key = Key, vsn = Vsn}=Entity, Opts) ->
   call(ambit_req_create, Key, {create, Entity#entity{vsn = uid:vclock(Vsn)}}, Opts).

%%
%% free service on the cluster
%%  Options
%%    w - number of succeeded writes
-spec(free/1 :: (entity()) -> entity() | {error, any()}).
-spec(free/2 :: (entity(), list()) -> entity() | {error, any()}).

free(Entity) ->
   ambitz:free(Entity, []).

free(#entity{key = Key, vsn = Vsn}=Entity, Opts) ->
   call(ambit_req_remove, Key, {remove, Entity#entity{vsn = uid:vclock(Vsn)}}, Opts).

%%
%% lookup service on the cluster
%%  Options
%%    r - number of succeeded reads
-spec(lookup/1 :: (key() | entity()) -> entity() | {error, any()}).
-spec(lookup/2 :: (key() | entity(), any()) -> entity() | {error, any()}).

lookup(Key) ->
   ambitz:lookup(Key, []).

lookup(Key, Opts)
 when is_binary(Key) orelse is_integer(Key) ->
   ambitz:lookup(entity(Key), Opts);

lookup(#entity{key = Key, vsn = Vsn}=Entity, Opts) ->
   call(ambit_req_lookup, Key, {lookup, Entity#entity{vsn = uid:vclock(Vsn)}}, Opts).



