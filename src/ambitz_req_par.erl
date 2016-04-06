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
%%   generic parallel request coordinator  
-module(ambitz_req_par).
-include("ambitz.hrl").
-include("include/ambitz.hrl").

-export([
   start_link/2
  ,init/1
  ,free/2
  ,ioctl/2
  ,idle/3
  ,active/3
]).
-export([
   call/5
]).

%%
-record(state, {
   pq  = undefined :: pid(),
   mod = undefined :: atom(),
   req = undefined :: any()
}).

%%%----------------------------------------------------------------------------   
%%%
%%% Factory
%%%
%%%----------------------------------------------------------------------------   

start_link(Pq, Mod) ->
   pipe:start_link(?MODULE, [Pq, Mod], []).

init([Pq, Mod]) ->
   lager:md([{ambit, req}]),
   {ok, idle, 
      #state{
         pq  = Pq,
         mod = Mod
      }
   }.

free(_, _) ->
   ok.

ioctl(_, _) ->
   throw(not_implemented).


%%%----------------------------------------------------------------------------   
%%%
%%% api
%%%
%%%----------------------------------------------------------------------------   

%%
%% synchronous request to distributed actors
call(Ring, Pool, Key, Req, Opts) ->
   #request{peer = Peer} = Request = ambitz_req:new(Ring, Key, Req, Opts),
   do_call(Peer, Pool, Request).

do_call([Head | Tail], Pool, #request{key = _Key, t = T} = Req) ->
   Peer = erlang:node( ek:vnode(peer, Head) ),
   ?DEBUG("ambitz [req]: init, key ~p, coord ~p", [_Key, Peer]),
   case pq:call({Pool, Peer}, Req, T) of
      {error, ebusy} ->
         do_call(Tail, Pool, Req);
      Result ->
         Result
   end;

do_call([], _Pool, _Req) ->
   {error, ebusy}.

%%%----------------------------------------------------------------------------   
%%%
%%% pipe
%%%
%%%----------------------------------------------------------------------------   

%%
%%
idle(#request{key = _Key} = Request0, Pipe, #state{mod = Mod} = State) ->
   ?DEBUG("ambitz [req]: recv key ~p, coord ~p", [_Key, erlang:node()]),
   Request1 = ambitz_req:coordinate(Mod, Pipe, Request0),
   case ambitz_req:ensure(Request1) of
      ok ->
         {next_state, active,
            State#state{req = ambitz_req:cast(Request1)}
         };

      {error, Reason} ->
         ?DEBUG("ambitz [req]: failure, key ~p, coord ~p, reason ~p", [Key, erlang:node(), Reason]),
         pipe:ack(Pipe, {error, [Reason]}),
         {next_state, idle, State}
   end.

%% 
%%
active({Tx, Value}, _Pipe, #state{pq = Pq, req = Request0} = State) ->
   case ambitz_req:propose(Tx, Value, Request0) of
      %% each peer replied or failed
      {eof, Request1} ->
         pipe:send(Pq, {release, self()}),
         {next_state, idle, 
            State#state{req = ambitz_req:free( ambitz_req:commit(Request1) )}
         };      

      %% sloppy quorum criteria met
      {eoq, Request1} ->
         {next_state, active, 
            State#state{req = ambitz_req:commit(Request1)}
         };

      %% continue execution
      {run, Request1} ->
         {next_state, active, 
            State#state{req = Request1}
         }
   end;

active({'DOWN', Ref, _, _, _Reason}, _Pipe, #state{pq = Pq, req = Request0} = State) ->
   case ambitz_req:propose(Ref, {error, peerdown}, Request0) of
      %% each peer replied or failed
      {eof, Request1} ->
         pipe:send(Pq, {release, self()}),
         {next_state, idle, 
            State#state{req = ambitz_req:free( ambitz_req:commit(Request1) )}
         };
      
      %% continue execution
      {_,   Request1} ->
         {next_state, active, 
            State#state{req = Request1}
         }
   end;

active(timeout, _Pipe, #state{req = Request0} = State) ->
   {stop, normal, 
      State#state{
         req = ambitz_req:free(
            ambitz_req:commit(
               ambitz_req:accept({error, timeout}, undefined, Request0)
            )
         )
      }
   }.

%%%----------------------------------------------------------------------------   
%%%
%%% private
%%%
%%%----------------------------------------------------------------------------   

