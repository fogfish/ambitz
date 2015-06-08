
%%
%% ambit entity
-record(entity, {
   key  = undefined :: binary(),       %% entity key
   val  = undefined :: any(),          %% entity val
   vsn  = []        :: uid:vclock()    %% entity dotted version
}).
