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
%%   LWW-Register
-module(ambitz_crdt_lwwregister).

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
   [undefined|uid:z()].

%%
%%
get(_Lens, Register) ->
   hd(Register).

%%
%%
put(_Lens, X, _Register) ->
   [X|uid:g()].

%%
%%
descend(A, B) ->
   tl(A) =< tl(B).

%%
%%
join(A, B) ->
   case tl(A) =< tl(B) of
      true  -> B;
      false -> A
   end.
