%% @description
%%   distributed actors
-module(ambitz_app).
-behaviour(application).
-include("ambitz.hrl").

-export([
   start/2
  ,stop/1
]).

%%
%%
start(_Type, _Args) ->
   case opts:val(ring, undefined, ambitz) of
      undefined ->
         {ok, self()};
      Ring      ->
         ek:seed(opts:val(seed, [], ambitz)),
         ek:create(?CONFIG_RING, Ring)
   end.

%%
%%
stop(_State) ->
   ok.
