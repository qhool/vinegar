-module(transactable).

-include("transactable.hrl").

-type(tid() :: #tid{}).
-type(transaction_store() :: {atom()|_}).

-export_types([tid/0, transaction_store/0]).

-callback(prepare(Tid :: tid(), Store :: transaction_store()) ->
                 transaction_store()).
-callback(commit(Tid :: tid(), Store :: transaction_store()) ->
                 transaction_store()).
-callback(rollback(Tid :: tid(), Store :: transaction_store()) ->
                 transaction_store()).

-export([new_tid/0, commit_id/1]).

new_tid() ->
    #tid{ t = -1 * erlang:monotonic_time(),
          umi = -1 * erlang:unique_integer([monotonic]),
          off = erlang:time_offset()
        }.

commit_id(Tid) ->
    Tid#tid{ t = -1 * erlang:monotonic_time(),
             off = erlang:time_offset()
           }.
