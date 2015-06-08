
%%
%% request timeout in milliseconds
-ifndef(CONFIG_TIMEOUT_REQ).
-define(CONFIG_TIMEOUT_REQ,  5000).
-endif.

%%
%% default R/W parameters
-define(CONFIG_W,      1).
-define(CONFIG_R,      1).
-define(CONFIG_N,      1).

%%
%% actor management application
-ifndef(CONFIG_RING).
-define(CONFIG_RING, ambit).
-endif.

%%
%% ambit entity
-record(entity, {
   key  = undefined :: binary(),       %% entity key
   val  = undefined :: any(),          %% entity val
   vsn  = []        :: uid:vclock()    %% entity dotted version
}).


%% 
%% logger macros
%%   debug, info, notice, warning, error, critical, alert, emergency
-ifndef(EMERGENCY).
-define(EMERGENCY(Fmt, Args), lager:emergency(Fmt, Args)).
-endif.

-ifndef(ALERT).
-define(ALERT(Fmt, Args), lager:alert(Fmt, Args)).
-endif.

-ifndef(CRITICAL).
-define(CRITICAL(Fmt, Args), lager:critical(Fmt, Args)).
-endif.

-ifndef(ERROR).
-define(ERROR(Fmt, Args), lager:error(Fmt, Args)).
-endif.

-ifndef(WARNING).
-define(WARNING(Fmt, Args), lager:warning(Fmt, Args)).
-endif.

-ifndef(NOTICE).
-define(NOTICE(Fmt, Args), lager:notice(Fmt, Args)).
-endif.

-ifndef(INFO).
-define(INFO(Fmt, Args), lager:info(Fmt, Args)).
-endif.

-ifdef(CONFIG_DEBUG).
   -define(DEBUG(Str, Args), lager:debug(Str, Args)).
-else.
   -define(DEBUG(Str, Args), ok).
-endif.

