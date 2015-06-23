
%%
%% ambit entity
-record(entity, {
   ring  = ambit     :: atom(),         %% entity ring
   key   = undefined :: binary(),       %% entity key
   val   = undefined :: any(),          %% entity val
   vsn   = []        :: uid:vclock(),   %% entity dotted version
   vnode = []        :: [ek:vnode()]    %% list of vnode
}).
