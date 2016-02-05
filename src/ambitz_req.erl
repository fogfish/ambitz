%% @description
%%   generic request coordinator
-module(ambitz_req).
-include("ambitz.hrl").

-export([
   start_link/1
  ,init/1
  ,free/2
  ,ioctl/2
  ,idle/3
  ,active/3
]).
-export([
   call/5
  % ,cast/4
]).

%%%----------------------------------------------------------------------------   
%%%
%%% Factory
%%%
%%%----------------------------------------------------------------------------   

start_link(Mod) ->
   pipe:start_link(?MODULE, [Mod], []).

init([Mod]) ->
   lager:md([{ambit, req}]),
   {ok, idle, #{mod => Mod}}.

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
   Peers = ek:successors(Ring, Key),
   do_call(Peers, Pool, {req, Peers, Key, Req, Opts}, Opts).

do_call([Head | Tail], Pool, Req, Opts) ->
   Peer = erlang:node( ek:vnode(peer, Head) ),
   case 
      %% @todo: remove deps to pq
      pq:call({Pool, Peer}, Req, opts:val(t, ?CONFIG_TIMEOUT_REQ, Opts))
   of
      {error, ebusy} ->
         do_call(Tail, Pool, Req, Opts);
      Result ->
         Result
   end;

do_call([], _Pool, _Req, _Opts) ->
   {error, ebusy}.

% %%
% %% asynchronous request to distributed actors
% cast(Mod, Key, Req, Opts) ->
%    cast(ek:successors(ambit, Key), Mod, Key, Req, Opts).
% %%
% %%
% cast([Vnode | T], Mod, Key, Req, Opts) ->
%    %% @todo: fucking major bottneck
%    %% @todo: lease involves extra RTT to service,
%    %%         design pq / peer api to mitigate the issue
%    case Mod:lease(Vnode) of
%       {error, _} ->
%          cast(T, Mod, Key, Req, Opts);
%       UoW ->
%          pipe:cast(pq:pid(UoW), 
%             {req, UoW, Key, Req, Opts}
%          )
%    end;

% cast([], _Mod, _Key, _Req, _Opts) ->
%    {error, ebusy}.

%%%----------------------------------------------------------------------------   
%%%
%%% pipe
%%%
%%%----------------------------------------------------------------------------   

%%
%%
idle({req, Peers, Key, Req, Opts}, Pipe, #{mod := Mod}) ->
   ?DEBUG("[~p] request ~p ~p", [self(), Key, Req]),
   case Mod:ensure(Peers, Key, Opts) of
      ok ->
         {next_state, active,
            req_cast(Peers, Key, Req,
               req_new(Mod, Pipe, Opts)
            )
         };

      {error, Reason} ->
         pipe:ack(Pipe, {error, [Reason]}),
         {next_state, idle, #{mod => Mod}}
   end.


%% 
%%
active({Tx, Value}, _Pipe, {List, Req0}) ->
   case lists:keytake(Tx, 3, List) of
      {value, {Ref, Peer, Tx},   []} ->
         erlang:demonitor(Ref, [flush]),
         {next_state, idle, 
            req_free(
               req_commit(
                  req_accept(Value, Peer, Req0)
               )
            )
         };
      {value, {Ref, Peer, Tx}, Tail} ->
         erlang:demonitor(Ref, [flush]),
         {next_state, active, 
            {Tail, req_accept(Value, Peer, Req0)}
         }
   end;

active({'DOWN', Ref, _, _, _Reason}, _Pipe,  {List, Req0}) ->
   case lists:keytake(Ref, 1, List) of
      {value, {Ref, Peer, _Tx},   []} ->
         {next_state, idle, 
            req_free(
               req_commit(
                  req_accept({error, abort},  Peer, Req0)
               )
            )
         };
      {value, {Ref, Peer, _Tx}, Tail} ->
         {next_state, active,
            {Tail, req_accept({error, abort}, Peer, Req0)}
         }
   end;

active(timeout, _Pipe, {_, Req0}) ->
   {next_state, idle, 
      req_free(
         req_commit(
            req_accept({error, timeout}, undefined, Req0)
         )
      )
   }.


%%%----------------------------------------------------------------------------   
%%%
%%% private
%%%
%%%----------------------------------------------------------------------------   

%%
%% initialize empty multi-cast request
req_new(Mod, Pipe, Opts) ->
   #{
      mod   => Mod,
      pipe  => Pipe,
      n     => opts:val(r, opts:val(w, ?CONFIG_W, Opts), Opts),
      t     => opts:val(t, ?CONFIG_TIMEOUT_REQ, Opts),
      opts  => Opts,
      value => orddict:new()
   }.

%%
%%
req_free(#{mod := Mod}) ->
   #{mod => Mod}.

%%
%% cast request to each peer 
req_cast(Peers, Key, Req, #{mod := Mod, t := T, opts := Opts}=State) ->
   Opts1 = case lists:keyfind(tx, 1, Opts) of
      false   ->
         [{tx, Mod:guid(Key)}|Opts];
      {tx, _} ->
         Opts
   end,
   List = lists:map(
      fun(Peer) ->
         {Mod:monitor(Peer), Peer, Mod:cast(Peer, Key, Req, Opts1)}
      end,
      Peers
   ),
   {List, State#{key => Key, req => Req, t => tempus:timer(timeout, T)}}.

%%
%% accept vs merge
req_accept(Value, Peer, #{mod := Mod, key := _Key, value := Value0}=State) ->
   %% @todo: we need order rules
   {Hash, Unit} = Mod:unit(Value),
   Value1 = orddict:update(Hash, fun({List, Acc}) -> {[Peer | List], Mod:join(Unit, Acc)} end, {[Peer], Unit}, Value0),
   ?DEBUG("[~p] accept ~p ~p", [self(), _Key, Unit]),
   State#{value => Value1}.

%%
%%
req_commit(#{n := N, key := _Key, value := Value, opts := Opts, pipe := Pipe}=State) ->
   case
      lists:partition(
         fun({_, {List, _Val}}) -> length(List) >= N end, 
         lists:reverse(lists:sort(fun sort/2, Value))
      )
   of
      {[{_Hash, {_Peers, Result}} | Head], Tail} ->
         ?DEBUG("[~p] result ~p ~p (~p)~n", [self(), _Key, Result, length(_Peers)]),
         pipe:ack(Pipe, Result),
         % @todo: document read-repair option
         case opts:val(repair, undefined, Opts) of
            rr ->
               repair(Result, Head ++ Tail, State);
            _  ->
               ok
         end,
         State;
      
      {[], _} ->
         pipe:ack(Pipe, {error, unity}),
         State
   end.

repair({error, _}, _Peers, _State) ->
   ok;
repair(_, [], _State) ->
   ok;
repair(Result, Peers, #{mod := Mod, key := Key, req := Req}) ->
   Mod:repair(lists:flatten([X || {_, {X, _}} <- Peers]), Key, Req, Result).

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

