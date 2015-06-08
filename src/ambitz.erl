%% @doc
%%   native api to distributed actors
-module(ambitz).
-include("ambitz.hrl").

-export([behaviour_info/1]).
-export([start/0]).
-export([
   start_link/1,
   call/4
]).

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
      %% -spec(ensure/2 :: ([ek:vnode()], list()) -> false | [ek:vnode()]).
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
   ];
behaviour_info(_) ->
   undefined.

%%%----------------------------------------------------------------------------   
%%%
%%% interface
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
-spec(call/4 :: (atom(), binary(), any(), list()) -> any() | {error, any()}).

call(Pool, Key, Req, Opts) ->
   ambitz_req:call(Pool, Key, Req, Opts).


