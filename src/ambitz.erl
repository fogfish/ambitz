%%
%%   Copyright 2014 Dmitry Kolesnikov, All Rights Reserved
%%
%%   Licensed under the Apache License, Version 2.0 (the "License");
%%   you may not use this file except in compliance with the License.
%%   You may obtain a copy of the License at
%%
%%       http://www.apache.org/licenses/LICENSE-2.0
%%
%%   Unless required by applicable law or agreed to in writing, software
%%   distributed under the License is distributed on an "AS IS" BASIS,
%%   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%%   See the License for the specific language governing permissions and
%%   limitations under the License.
%%
%% @doc
%%   native api to distributed actors
-module(ambitz).
-include("ambitz.hrl").
-include("include/ambitz.hrl").

-export([behaviour_info/1]).
-export([start/0]).
-export([
   start_link/2,
   call/3,
   call/4,
   call/5
]).
-export([
   entity/1
  ,entity/2
  ,entity/3
]).
-export([
   spawn/1,
   spawn/2,
   lookup/1,
   lookup/2,
   whereis/1,
   whereis/2,
   free/1,
   free/2
]).
-export_type([entity/0]).

%%
%% data types
-type key()    :: binary().
-type entity() :: #entity{}.

%% 
%%  Common error reason
%%   ebusy - cluster run out of compute capacity, i/o pools exhausted
%%   unity - the quorum requirements of request is not achievable, 
%%           the replicas are not agreeing on the status. 
%%   [_]   - replica failure
-type reason() :: ebusy | unity | [_].

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
      %%
      %% -spec(guid/1 :: (any()) -> any()).
     ,{guid,     1}

      %%
      %% monitor transaction actor
      %%
      %% -spec(monitor/1 :: (ek:vnode()) -> reference()). 
     ,{monitor, 1}

      %%
      %% asynchronously cast request to transaction handler / actor
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
%% start pool of request coordinators, using module as pool identity
-spec(start_link/2 :: (atom(), integer()) -> {ok, pid()} | {error, any()}).

start_link(Mod, Capacity) ->
   pq:start_link(Mod, [
      {capacity, Capacity}
     ,{worker,   {ambitz_req_par, [Mod]}}
   ]). 


%%
%% request distributed actor
-spec(call/3 :: (atom(), binary(), any()) -> {ok, _} | {error, reason()}).
-spec(call/4 :: (atom(), binary(), any(), list()) -> {ok, _} | {error, reason()}).
-spec(call/5 :: (atom(), atom(), binary(), any(), list()) -> {ok, _} | {error, reason()}).

call(Pool, Key, Req) ->
   ambitz_req_par:call(ambit, Pool, Key, Req, []).
   
call(Pool, Key, Req, Opts) ->
   ambitz_req_par:call(ambit, Pool, Key, Req, Opts).

call(Ring, Pool, Key, Req, Opts) ->
   ambitz_req_par:call(Ring, Pool, Key, Req, Opts).

%%%----------------------------------------------------------------------------   
%%%
%%% api
%%%
%%%----------------------------------------------------------------------------   

%%
%% create casual context
-spec(entity/1 :: (binary()) -> entity()).

entity(Key) ->
   #entity{key = Key}.


%%
%% get property of casual context
-spec(entity/2 :: (atom(), entity()) -> any() | undefined).

entity(ring,    #entity{ring = Ring}) ->
   Ring;

entity(key,     #entity{key = Key}) ->
   Key;

entity(service, #entity{val = Service}) ->
   Service;

entity(vsn,     #entity{vsn = Vsn}) ->
   Vsn;

entity(vnode,   #entity{vnode = Vnode}) ->
   Vnode.

%%
%% set property of casual context
-spec(entity/3 :: (atom(), any(), entity()) -> entity()).

%% define ring
entity(ring, Ring, Entity) ->
   Entity#entity{ring = Ring};

%% define service specification as {Module, Function, Unit}
entity(service, {_, _, _} = Service, Entity) ->
   Entity#entity{val  = Service}.


%%
%% spawn service on the cluster
%%  Options
%%    w - number of succeeded writes
-spec(spawn/1 :: (entity()) -> {ok, entity()} | {error, any()}).
-spec(spawn/2 :: (entity(), list()) -> {ok, entity()} | {error, any()}).

spawn(Entity) ->
   ambitz:spawn(Entity, []).

spawn(#entity{ring = Ring, key = Key, vsn = Vsn}=Entity, Opts) ->
   call(Ring, ambit_req_create, Key, {create, Entity#entity{vsn = uid:vclock(Vsn)}}, Opts).

%%
%% free service on the cluster
%%  Options
%%    w - number of succeeded writes
-spec(free/1 :: (entity()) -> {ok, entity()} | {error, any()}).
-spec(free/2 :: (entity(), list()) -> {ok, entity()} | {error, any()}).

free(Entity) ->
   ambitz:free(Entity, []).

free(#entity{ring = Ring, key = Key, vsn = Vsn}=Entity, Opts) ->
   call(Ring, ambit_req_remove, Key, {remove, Entity#entity{vsn = uid:vclock(Vsn)}}, Opts).

%%
%% lookup service on the cluster
%%  Options
%%    r - number of succeeded reads
-spec(lookup/1 :: (key() | entity()) -> {ok, entity()} | {error, any()}).
-spec(lookup/2 :: (key() | entity(), any()) -> {ok, entity()} | {error, any()}).

lookup(Key) ->
   ambitz:lookup(Key, []).

lookup(Key, Opts)
 when is_binary(Key) orelse is_integer(Key) ->
   ambitz:lookup(entity(Key), Opts);

lookup(#entity{ring = Ring, key = Key, vsn = Vsn}=Entity, Opts) ->
   call(Ring, ambit_req_lookup, Key, {lookup, Entity#entity{vsn = uid:vclock(Vsn)}}, Opts).

%%
%% lookup discover process id on the cluster
%%  Options
%%    r - number of succeeded reads
-spec(whereis/1 :: (key() | entity()) -> {ok, entity()} | {error, any()}).
-spec(whereis/2 :: (key() | entity(), any()) -> {ok, entity()} | {error, any()}).

whereis(Key) ->
   ambitz:whereis(Key, []).

whereis(Key, Opts)
 when is_binary(Key) orelse is_integer(Key) ->
   ambitz:whereis(entity(Key), Opts);

whereis(#entity{ring = Ring, key = Key, vsn = Vsn}=Entity, Opts) ->
   call(Ring, ambit_req_whereis, Key, {process, Entity#entity{vsn = uid:vclock(Vsn)}}, Opts).


