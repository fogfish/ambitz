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
   ek:seed(opts:val(seed, [], ambitz)).

%%
%%
stop(_State) ->
   ok.
