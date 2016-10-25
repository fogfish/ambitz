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
   call/2,
   call/3
]).
-export([
   spawn/2
  ,spawn/3
  ,spawn/4
  ,lookup/1
  ,lookup/2
  ,lookup/3,
   whereis/1
  ,whereis/2
  ,whereis/3
  ,put/3
  ,put/4
  ,put/5
  ,get/2
  ,get/3
  ,get/4
  ,free/1
  ,free/2
  ,free/3
]).

-export_type([entity/0]).

%%
%% data types
-type ring()   :: atom().
-type key()    :: binary().
-type lens()   :: _.
-type spec()   :: mfa().
-type opts()   :: [_].
-type entity() :: #entity{}.

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
      %% monitor transaction actor
      %%
      %% -spec(monitor/1 :: (ek:vnode()) -> reference()). 
      {monitor, 1}

      %%
      %% asynchronously cast request to transaction handler / actor
      %%
      %% -spec(cast/4 :: (ek:vnode(), entity(), opts()) -> reference()). 
     ,{cast,    3} 

      %%
      %% optional - read repair
      %% 
      %% -spec(repair/2 :: (ek:vnode(), entity(), opts()) -> ok).
      %% {repair, 3}
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
-spec start_link(atom(), integer()) -> {ok, pid()} | {error, any()}.

start_link(Mod, Capacity) ->
   %% @todo: use unbounded pool of workers
   pq:start_link(Mod, [
      {capacity, Capacity}
     ,{strategy, lifo}
     ,{worker,   {ambitz_req_par, [Mod]}}
   ]). 


%%
%% request distributed actor
-spec call(atom(), entity()) -> {ok, entity()}.
-spec call(atom(), entity(), opts()) -> {ok, entity()}.

   
call(Pool, Entity) ->
   ambitz_req_par:call(Pool, Entity, []).

call(Pool, Entity, Opts) ->
   ambitz_req_par:call(Pool, Entity, Opts).

%%%----------------------------------------------------------------------------   
%%%
%%% api
%%%
%%%----------------------------------------------------------------------------   

%%
%% spawn actor on the cluster
%%  Options
%%    w - 
%%    t -
-spec spawn(key(), spec()) -> {ok, entity()}.
-spec spawn(ring(), key(), spec()) -> {ok, entity()}.
-spec spawn(ring(), key(), spec(), opts()) -> {ok, entity()}.

spawn(Key, Spec) ->
   ambitz:spawn(ambit, Key, Spec).

spawn(Ring, Key, Spec) ->
   ambitz:spawn(Ring, Key, Spec, []).

spawn(Ring, Key, Spec, Opts) ->
   call(ambit_req_spawn,  
      #entity{ring = Ring, key = Key, val = crdts:update(Spec, crdts:new(lwwreg))},
      Opts
   ).


%%
%% terminate (free) actor on the cluster
%%  Options
%%    w - 
%%    t -
-spec free(key()) -> {ok, entity()}.
-spec free(ring(), key()) -> {ok, entity()}.
-spec free(ring(), key(), opts()) -> {ok, entity()}.

free(Key) ->
   ambitz:free(ambit, Key).

free(Ring, Key) ->
   ambitz:free(Ring, Key, []).

free(Ring, Key, Opts) ->
   call(ambit_req_free,  
      #entity{ring = Ring, key = Key, val = crdts:new(lwwreg)},
      Opts
   ).

%%
%% lookup actor on the cluster
%%  Options
%%    r - 
%%    t -
-spec lookup(key()) -> {ok, entity()}.
-spec lookup(ring(), key()) -> {ok, entity()}.
-spec lookup(ring(), key(), opts()) -> {ok, entity()}.

lookup(Key) ->
   ambitz:lookup(ambit, Key).

lookup(Ring, Key) ->
   ambitz:lookup(Ring, Key, []).

lookup(Ring, Key, Opts) ->
   call(ambit_req_lookup,  
      #entity{ring = Ring, key = Key},
      Opts
   ).

%%
%% discover actor processes on the cluster
%%  Options
%%    r - 
%%    t -
-spec whereis(key()) -> {ok, entity()}.
-spec whereis(ring(), key()) -> {ok, entity()}.
-spec whereis(ring(), key(), opts()) -> {ok, entity()}.

whereis(Key) ->
   ambitz:whereis(ambit, Key).

whereis(Ring, Key) ->
   ambitz:whereis(Ring, Key, []).

whereis(Ring, Key, Opts) ->
   call(ambit_req_whereis, 
      #entity{ring = Ring, key = Key, val = crdts:new(gsets)},
      Opts
   ).

%%
%%
-spec put(key(), lens(), crdts:crdt()) -> {ok, entity()}.
-spec put(ring(), key(), lens(), crdts:crdt()) -> {ok, entity()}.
-spec put(ring(), key(), lens(), crdts:crdt(), opts()) -> {ok, entity()}.

put(Key, Lens, Value) ->
   ambitz:put(ambit, Key, Lens, Value).

put(Ring, Key, Lens, Value) ->
   ambitz:put(Ring, Key, Lens, Value, []).

put(Ring, Key, Lens, Value, Opts) ->
   call(ambit_req_put,
      #entity{ring = Ring, key = Key, val = Value},
      [{lens, Lens}|Opts]
   ).
   
%%
%%
-spec get(key(), lens()) -> {ok, entity()}.
-spec get(ring(), key(), lens()) -> {ok, entity()}.
-spec get(ring(), key(), lens(), opts()) -> {ok, entity()}.

get(Key, Lens) ->
   ambitz:get(ambit, Key, Lens).

get(Ring, Key, Lens) ->
   ambitz:get(Ring, Key, Lens, []).

get(Ring, Key, Lens, Opts) ->
   call(ambit_req_get, 
      #entity{ring = Ring, key = Key},
      [{lens, Lens}|Opts]
   ).
