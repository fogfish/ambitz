%%
%%   Copyright 2016 Dmitry Kolesnikov, All Rights Reserved
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
%%   G-Counter
-module(ambitz_crdt_gcounter).

-export([
   new/0,
   get/2,
   put/3,
   descend/2,
   join/2
]).

%%
%%
new() -> 
   [].

%%
%%
get(_Lens, Counter) ->
   lists:sum([X || {_, X} <- Counter]).

%%
%%
put(_Lens, X, Counter) ->
   Node = erlang:node(),
   case lists:keytake(Node, 1, Counter) of
      false ->
         [{Node, X} | Counter];
      {value, {Node, Y}, Tail} ->
         [{Node, X + Y} | Tail]
   end.

%%
%%
descend([], _) ->
   true;
descend([{Node, _} = X | A], B) ->
   case lists:keyfind(Node, 1, B) of
      false ->
         false;
      Y  ->
         (X =< Y) andalso descend(A, B)
   end.   

%%
%%
join(A, B) ->
   join1(lists:keysort(1, A), lists:keysort(1, B)).   

join1([{NodeA, _} = X | A], [{NodeB, _} | _] = B)
 when NodeA < NodeB ->
   [X | join1(A, B)];

join1([{NodeA, _} | _] = A, [{NodeB, _} = X | B])
 when NodeA > NodeB ->
   [X | join1(A, B)];

join1([{Node, X} | A], [{Node, Y} | B]) ->
   [{Node, erlang:max(X, Y)} | join1(A, B)];

join1([], B) ->
   B;

join1(A, []) ->
   A.
