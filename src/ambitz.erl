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
% -export([
%    new/1
%   ,new/2
%   ,put/2
%   ,put/3
%   ,get/1
%   ,get/2
%   ,descend/2
%   ,join/2
%   ,key/1
%   ,vnode/1
%   ,vnode/2
%   ,ring/1
%   ,ring/2
% ]).
-export([
   % actor/1,
   % actor/2,
   spawn/2,
   spawn/3,
   lookup/1,
   lookup/2,
   whereis/1,
   whereis/2,
   put/3,
   put/4,
   get/2,
   get/3,
   % ioctl/2,
   % ioctl/3,
   free/1,
   free/2
]).

-export_type([entity/0]).

%%
%% data types
-type ring()   :: atom().
-type key()    :: binary().
-type lens()   :: _.
-type spec()   :: mfa().
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
%%% commutativity replicated data type (CRDT)
%%%
%%%----------------------------------------------------------------------------   

%%
%% http://hal.upmc.fr/file/index/docid/555588/filename/techreport.pdf

%% 
%% create new data type instance
% -spec new(atom()) -> entity().
% -spec new(atom(), key()) -> entity().

% new(CRDT) ->
%    new(CRDT, undefined).

% new(gcounter, Key) ->
%    new(ambitz_crdt_gcounter, Key);

% new(lww_register, Key) ->
%    new(ambitz_crdt_lwwregister, Key);

% new(gset, Key) ->
%    new(ambitz_crdt_gset, Key);

% new(CRDT, Key) ->
%    #entity{type = CRDT, key = Key, val = CRDT:new()}.   

%%
%% update operation of data type
% -spec put(_, entity()) -> entity().
% -spec put(_, _, entity()) -> entity().

% put(X, #entity{} = Entity) ->
%    ambitz:put(undefined, X, Entity).

% put(Lens, X, #entity{type = CRDT, val = Val} = Entity) ->
%    Entity#entity{val = CRDT:put(Lens, X, Val)}.

%%
%% query local state of data type
% -spec get(entity()) -> _.
% -spec get(_, entity()) -> _.

% get(#entity{} = Entity) ->
%    ambitz:get(undefined, Entity).

% get(Lens, #entity{type = CRDT, val = Val}) ->
%    CRDT:get(Lens, Val).

%%
%% compare values, return if A =< B in semi-lattice
% -spec descend(entity(), entity()) -> true | false.

% descend(#entity{type = CRDT, val = A}, #entity{type = CRDT, val = B}) ->
%    CRDT:descend(A, B).

%%
%% join two value(s)
% -spec join(entity(), entity()) -> entity().

% join(#entity{type = CRDT, val = A} = Entity, #entity{type = CRDT, val = B}) ->
%    Entity#entity{val = CRDT:join(A, B)}.

%%
%%
% -spec key(entity()) -> _.

% key(#entity{key = Key}) ->
%    Key.

%%
%%
% -spec vnode(entity()) -> [ek:vnode()].

% vnode(#entity{vnode = Vnode}) ->
%    Vnode.

%%
%%
% -spec vnode(ek:vnode(), entity()) -> entity().

% vnode(Vnode, #entity{} = Entity) ->
%    Entity#entity{vnode = Vnode}.


%%
%%
% -spec ring(entity()) -> atom().

% ring(#entity{ring = Ring}) ->
%    Ring.

%%
%%
% -spec ring(atom(), entity()) -> entity().

% ring(Ring, #entity{} = Entity) ->
%    Entity#entity{ring = Ring}.   


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
   pq:start_link(Mod, [
      {capacity, Capacity}
     ,{worker,   {ambitz_req_par, [Mod]}}
   ]). 


%%
%% request distributed actor
-spec call(atom(), binary(), any()) -> {ok, _} | {error, reason()}.
-spec call(atom(), binary(), any(), list()) -> {ok, _} | {error, reason()}.
-spec call(atom(), atom(), binary(), any(), list()) -> {ok, _} | {error, reason()}.

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
%% create actor specification
% -spec actor(key()) -> entity().
% -spec actor(key(), spec()) -> entity().

% actor(Key) ->
%    ambitz:put(undefined, ambitz:new(lww_register, Key)).

% actor(Key, {_, _, _} = Spec) ->
%    ambitz:put(Spec, ambitz:new(lww_register, Key)).


%%
%% spawn actor on the cluster
-spec spawn(key(), spec()) -> {ok, entity()}.
-spec spawn(ring(), key(), spec()) -> {ok, entity()}.

spawn(Key, Spec) ->
   ambitz:spawn(ambit, Key, Spec).

spawn(Ring, Key, Spec) ->
   call(Ring, ambit_req_spawn, Key, 
      {'$ambitz', spawn, 
         #entity{ring = Ring, key = Key, val = crdts:update(Spec, crdts:new(lwwreg))}
      },
      []
   ).

% -spec spawn(entity()) -> {ok, entity()}.
% -spec spawn(entity(), [_]) -> {ok, entity()}.

% spawn(Entity) ->
%    ambitz:spawn(Entity, []).

% spawn(#entity{ring = Ring, key = Key}=Entity, Opts) ->
%    call(Ring, ambit_req_create, Key, {'$ambitz', spawn, Entity}, Opts).

%%
%% terminate (free) actor on the cluster
-spec free(key()) -> {ok, entity()}.
-spec free(ring(), key()) -> {ok, entity()}.

free(Key) ->
   ambitz:free(ambit, Key).

free(Ring, Key) ->
   call(Ring, ambit_req_free, Key, 
      {'$ambitz', free, 
         #entity{ring = Ring, key = Key, val = crdts:new(lwwreg)}
      },
      []
   ).


% -spec free(entity()) -> {ok, entity()}.
% -spec free(entity(), [_]) -> {ok, entity()}.

% free(Entity) ->
%    ambitz:free(Entity, []).

% free(#entity{ring = Ring, key = Key}=Entity, Opts) ->
%    call(Ring, ambit_req_remove, Key, {'$ambitz', free, Entity}, Opts).


%%
%% lookup actor on the cluster
-spec lookup(key()) -> {ok, entity()}.
-spec lookup(ring(), key()) -> {ok, entity()}.

lookup(Key) ->
   ambitz:lookup(ambit, Key).

lookup(Ring, Key) ->
   call(Ring, ambit_req_lookup, Key, 
      {'$ambitz', lookup, 
         #entity{ring = Ring, key = Key}
      },
      []
   ).

% -spec lookup(entity()) -> {ok, entity()}.
% -spec lookup(entity(), [_]) -> {ok, entity()}.

% lookup(Key) ->
%    ambitz:lookup(Key, []).

% lookup(#entity{ring = Ring, key = Key}=Entity, Opts) ->
%    call(Ring, ambit_req_lookup, Key, {'$ambitz', lookup, Entity}, Opts).

%%
%% discover actor processes on the cluster
-spec whereis(key()) -> {ok, entity()}.
-spec whereis(ring(), key()) -> {ok, entity()}.

whereis(Key) ->
   ambitz:whereis(ambit, Key).

whereis(Ring, Key) ->
   call(Ring, ambit_req_whereis, Key, 
      {'$ambitz', whereis, 
         #entity{ring = Ring, key = Key, val = crdts:new(gset)}
      },
      []
   ).

% -spec whereis(entity()) -> {ok, entity()}.
% -spec whereis(entity(), [_]) -> {ok, entity()}.

% whereis(Key) ->
%    ambitz:whereis(Key, []).

% whereis(#entity{ring = Ring, key = Key}=Entity, Opts) ->
%    call(Ring, ambit_req_whereis, Key, {'$ambitz', whereis, Entity}, Opts).

-spec put(key(), lens(), crdts:crdt()) -> {ok, entity()}.
-spec put(ring(), key(), lens(), crdts:crdt()) -> {ok, entity()}.

put(Key, Lens, Value) ->
   ambitz:put(ambit, Key, Lens, Value).

put(Ring, Key, Lens, Value) ->
   call(Ring, ambit_req_put, Key, 
      {'$ambitz', {put, Lens}, 
         #entity{ring = Ring, key = Key, val = Value}
      },
      []
   ).
   

-spec get(key(), lens()) -> {ok, entity()}.
-spec get(ring(), key(), lens()) -> {ok, entity()}.

get(Key, Lens) ->
   ambitz:get(ambit, Key, Lens).

get(Ring, Key, Lens) ->
   call(Ring, ambit_req_get, Key, 
      {'$ambitz', {get, Lens}, 
         #entity{ring = Ring, key = Key}
      },
      []
   ).

%% do we need opts (r, w for this data types) ?
%% can we use descend / merge for conflict resolution ?

%%
%% configure actor
% -spec ioctl(_, entity()) -> {ok, entity()}.
% -spec ioctl(_, entity(), [_]) -> {ok, entity()}.

% ioctl(Lens, Entity) ->
%    ambitz:ioctl(Lens, Entity, []).

% ioctl(Lens, #entity{ring = Ring, key = Key}=Entity, Opts) ->
%    call(Ring, ambit_req_ioctl, Key, {'$ambitz', {ioctl, Lens}, Entity}, Opts).


