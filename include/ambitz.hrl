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

%%
%% ambit entity
-record(entity, {
   ring  = ambit     :: atom(),         %% unique identity of ring
   key   = undefined :: binary(),       %% unique identity of actor
   val   = undefined :: crdts:crdt(),   %% commutativity replicated data type
   vnode = []        :: [ek:vnode()]    %% list of v-node
}).

%%
%% ambit request (client -> coordinator)
%% @todo: design algebraic data type(s)
-record(request, {
   entity  = undefined :: #entity{}     %% request entity
  ,opts    = undefined :: [_]           %% raw options to pass

   %% request flags
  ,n       = undefined :: integer()     %% number of successful responses
  ,t       = undefined :: timeout()     %% request timeout
  ,commit  = undefined :: atom()        %% algorithms to commit request
}).

-record(cast, {
   mod     = undefined :: atom()         %% module implementing request
  ,pipe    = undefined :: any()          %% pipe to communicate result back
  ,tx      = undefined :: [_]            %% list of on-going transactions
  ,t       = undefined :: timeout()      %% request timeout
  ,n       = undefined :: integer()      %% number of successful responses
  ,value   = []        :: [#entity{}]    %% accepted successful result
  ,error   = []        :: [{error,_}]    %% accepted failures
  ,request = undefined :: #request{}     %% original request     
}).




