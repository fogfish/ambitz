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
%%   request data structure
-module(ambitz_req).
-include("ambitz.hrl").
-include("include/ambitz.hrl").

-export([
   new/4,
   free/1,
   coordinate/3,
   ensure/1,
   cast/1,
   propose/3,   
   commit/1
]).


%%
%% create new request
new(Ring, Key, Req, Opts) ->
   #request{
      peer    = ek:successors(Ring, Key),
      key     = Key,
      uid     = opts:val(tx, undefined, Opts),
      req     = Req,
      n       = opts:val(r, opts:val(w, ?CONFIG_W, Opts), Opts),
      t       = opts:val(t, ?CONFIG_TIMEOUT_REQ, Opts),
      commit  = opts:val(commit, undefined, Opts)
   }.

%%
%%
free(_) ->
   undefined.

%%
%% bind request with coordinator process
coordinate(Mod, Pipe, #request{uid = undefined, key = Key} = Request) ->
   Request#request{
      uid   = Mod:guid(Key),
      mod   = Mod,
      pipe  = Pipe,
      value = orddict:new()
   };

coordinate(Mod, Pipe, #request{} = Request) ->
   Request#request{
      mod   = Mod,
      pipe  = Pipe,
      value = orddict:new()
   }.

%%
%% ensure presence of actor in the cluster
ensure(#request{peer = Peer, key = Key, mod = Mod}) ->
   %% @todo: [] kept for api compatibility
   Mod:ensure(Peer, Key, []).

%%
%% cast request to each peer 
cast(#request{peer = Peer, key = Key, req = Req, mod = Mod, t = T} = Request) ->
   TxList = lists:map(
      fun(Px) ->
         ?DEBUG("ambitz [req]: cast, key ~p, peer ~p", [Key, Px]),
         %% @todo: [] kept for api compatibility
         {Mod:monitor(Px), Px, Mod:cast(Px, Key, Req, [])}
      end,
      Peer
   ),
   Request#request{
      tx = TxList,
      t  = tempus:timer(timeout, T)
   }.

%%
%% propose tx / peer value 
propose(Tx, {error, peerdown} = Value, Request) ->
   propose(1, Tx, Value, Request);

propose(Tx, Value, Request) ->
   propose(3, Tx, Value, Request).

propose(I, Tx, Value, #request{tx = TxList, n = N} = Request0) ->
   {value, {Ref, Peer, Tx}, Tail} = lists:keytake(Tx, I, TxList),
   erlang:demonitor(Ref, [flush]),
   case {length(Tail), accept(Value, Peer, Request0)} of
      %% each peer proposed the value
      {0, {_, Request1}} ->
         {eof, Request1#request{tx = Tail}};

      %% accepted value meet sloppy quorum criteria
      {_,  {X, Request1}} when X >= N ->
         {eoq, Request1#request{tx = Tail}};
      
      %% no quorum yet, wait for other peers
      {_,  {_, Request1}} ->
         {run, Request1#request{tx = Tail}}
   end.

%%
%% accept proposed result 
%% @todo: we need to order result based on peer, agreements, etc.
accept(Value, Peer, #request{mod = Mod, key = _Key, value = Value0}=Req) ->
   {Hash, Unit} = Mod:unit(Value),
   Value1 = orddict:update(Hash, 
      fun({List, Acc}) -> 
         {[Peer | List], Mod:join(Unit, Acc)}
      end, 
      {[Peer], Unit}, 
      Value0
   ),
   {List, _} = orddict:fetch(Hash, Value1),
   N = length(List),
   ?DEBUG("ambitz [req]: accept, key ~p (~p), coord ~p, peer ~p", [_Key, N, erlang:node(), Peer]),
   {N, Req#request{value = Value1}}.



%%
%%
commit(#request{pipe = undefined} = Request) ->
   Request;

commit(#request{key = _Key, value = Value, n = N, pipe = Pipe}=Request) ->
   case
      lists:partition(
         fun({_, {List, _Val}}) -> length(List) >= N end, 
         lists:reverse(lists:sort(fun sort/2, Value))
      )
   of
      {[{_Hash, {_Peers, Result}} | Head], Tail} ->
         ?DEBUG("[~p] result ~p ~p (~p)~n", [self(), _Key, Result, length(_Peers)]),
         pipe:ack(Pipe, Result),
         req_post_commit(Result, Head ++ Tail, Request),
         Request#request{pipe = undefined};
      
      {[], _} ->
         pipe:ack(Pipe, {error, unity}),
         Request#request{pipe = undefined}
   end.

%%
%%
req_post_commit(_Value, _Peer, #request{commit = undefined}) ->
   ok;

req_post_commit({error, _}, _Peer, #request{commit = repair}) ->
   ok;
req_post_commit(_Value,  [], #request{commit = repair}) ->
   ok;
req_post_commit(Value, Peer, #request{commit = repair, mod = Mod, key = Key, req = Req}) ->
   Mod:repair(lists:flatten([X || {_, {X, _}} <- Peer]), Key, Req, Value).

%%
%%
sort({_, {A, _Val}}, {_, {B, _Val}})
 when length(A) =/= length(B) ->
   length(A) < length(B);
sort({_, {_, {error, _}}}, {_, {_, _}}) ->
   true;
sort({_, {_, undefined}},  {_, {_, _}}) ->
   true;
sort({_, _},  {_, {_, _}}) ->
   false.

