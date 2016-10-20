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
   new/2,
   free/1,
   coordinate/3,
   % ensure/1,
   cast/1,
   propose/3,   
   commit/1
]).


%%
%% create new request
% new(Ring, Key, Req, Opts) ->
new(Entity, Opts) ->
   #request{
      entity  = Entity,
      opts    = Opts,
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
coordinate(Mod, Pipe, #request{} = Request) ->
   #cast{
      mod     = Mod,
      pipe    = Pipe,
      request = Request
   }.

%%
%% cast request to each peer 
cast(#cast{mod = Mod, request = #request{t = T, n = N, entity = Entity, opts = Opts}} = Cast) ->
   #entity{key = _Key, vnode = Vnodes} = Entity,
   Tx = [{Mod:monitor(Vnode), Vnode, Mod:cast(Vnode, Entity, Opts)} || Vnode <- Vnodes],
   ?DEBUG("ambitz [req]: cast, key ~p to ~p", [_Key, Vnodes]),
   Cast#cast{
      t  = tempus:timer(timeout, T), 
      n  = N, 
      tx = Tx
   }.

%%
%% propose peer result 
propose(Tx, {error, Reason}, Request) ->
   propose(1, Tx, Reason, Request);

propose(Tx, {ok, Entity}, Request) ->
   propose(3, Tx, Entity, Request).

propose(Id, Uid, Value, #cast{tx = TxList, n = N} = Request0) ->
   {value, {Ref, Vnode, _Tx}, TxTail} = lists:keytake(Uid, Id, TxList),
   erlang:demonitor(Ref, [flush]),
   case {length(TxTail), accept(Value, Vnode, Request0)} of
      %% each peer proposed the value
      {0, {_, Request1}} ->
         {eof, Request1#cast{tx = TxTail}};

      %% accepted value meet sloppy quorum criteria
      {_, {X, Request1}} when X =:= N ->
         {eoq, Request1#cast{tx = TxTail, n = N}};
      
      %% no quorum yet, wait for other peers
      {_, {_, Request1}} ->
         {run, Request1#cast{tx = TxTail, n = N}}
   end.

%%
%% accept proposed result 
accept(#entity{} = Value, _Vnode, #cast{value = List} = Cast) ->
   {length(List) + 1, Cast#cast{value = [Value|List]}};

accept(Value, Vnode, #cast{error = List} = Cast) ->
   {0, Cast#cast{error = [{Vnode, Value}|List]}}.

% accept(Value, Peer, #request{mod = Mod, key = _Key, value = Value0}=Req) ->
%    {Hash, Unit} = Mod:unit(Value),
%    Value1 = orddict:update(Hash, 
%       fun({List, Acc}) -> 
%          {[Peer | List], Mod:join(Unit, Acc)}
%       end, 
%       {[Peer], Unit}, 
%       Value0
%    ),
%    {List, _} = orddict:fetch(Hash, Value1),
%    N = length(List),
%    ?DEBUG("ambitz [req]: accept, key ~p (~p), coord ~p, peer ~p", [_Key, N, erlang:node(), Peer]),
%    {N, Req#request{value = Value1}}.



%%
%%
commit(#cast{pipe = undefined} = Request) ->
   Request;

commit(#cast{pipe = Pipe, value = [], error = Error} = Request) ->
   pipe:ack(Pipe, {error, Error}),
   Request#cast{pipe = undefined};

commit(#cast{pipe = Pipe, value = [Head|Tail]} = Request) ->
   Entity = lists:foldl(fun join/2, Head, Tail),   
   pipe:ack(Pipe, {ok, Entity}),
   finalize(Entity, Request),
   Request#cast{pipe = undefined}.

finalize(_, #cast{request = #request{commit = undefined}}) ->
   ok;
finalize(_, #cast{request = #request{commit = _}}) ->
   %% @todo: implement read repair
   ok.

join(#entity{vnode = VnodeA, val = A} = EntityA, #entity{vnode = VnodeB, val = B}) ->
   EntityA#entity{vnode = VnodeA ++ VnodeB, val = crdts:join(A, B)}.

   
%    case
%       lists:partition(
%          fun({_, {List, _Val}}) -> length(List) >= N end, 
%          lists:reverse(lists:sort(fun sort/2, Value))
%       )
%    of
%       {[{_Hash, {_Peers, Result}} | Head], Tail} ->
%          ?DEBUG("[~p] result ~p ~p (~p)~n", [self(), _Key, Result, length(_Peers)]),
         
%          req_post_commit(Result, Head ++ Tail, Request),
         
      
%       {[], _} ->
%          pipe:ack(Pipe, {error, unity}),
%          Request#request{pipe = undefined}
%    end.

% %%
% %%
% req_post_commit(_Value, _Peer, #request{commit = undefined}) ->
%    ok;

% req_post_commit({error, _}, _Peer, #request{commit = repair}) ->
%    ok;
% req_post_commit(_Value,  [], #request{commit = repair}) ->
%    ok;
% req_post_commit(Value, Peer, #request{commit = repair, mod = Mod, key = Key, req = Req}) ->
%    Mod:repair(lists:flatten([X || {_, {X, _}} <- Peer]), Key, Req, Value).

% %%
% %%
% sort({_, {A, _Val}}, {_, {B, _Val}})
%  when length(A) =/= length(B) ->
%    length(A) < length(B);
% sort({_, {_, {error, _}}}, {_, {_, _}}) ->
%    true;
% sort({_, {_, undefined}},  {_, {_, _}}) ->
%    true;
% sort({_, _},  {_, {_, _}}) ->
%    false.

