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
   val   = undefined :: any(),          %% commutativity replicated data type
   vnode = []        :: [ek:vnode()]    %% list of v-node
}).

%%
%% ambit request (client -> coordinator)
-record(request, {
   peer    = undefined :: [ek:vnode()],  %% list of peers to executed request
   key     = undefined :: binary(),      %% request key
   uid     = undefined :: any(),         %% unique transaction id (k-order)

   req     = undefined :: any(),         %% request payload
   value   = undefined :: [{_, _}],      %% request result
   tx      = undefined :: [_],           %% list of on-going transactions

   mod     = undefined :: atom(),        %% module implementing request
   pipe    = undefined :: any(),         %% pipe to communicate result back          
   n       = undefined :: integer(),     %% number of successful responses
   t       = undefined :: timeout(),     %% request timeout
   commit  = undefined :: atom()         %% post commit algorithms
}).




