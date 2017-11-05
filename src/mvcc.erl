%% basic data:
%%
%% {WriteT, Data}
%%
%% open transaction list (process/id, read time)
%%
%% operations:
%% latest committed version
%% latest version before T
%% earliest read ts
%%
%% transactional read -- add to txn list w/ read time
%% add version
%% mark version as committed
%%
%% garbage collect -- remove all committed versions before
-module(mvcc).
-include_lib("eunit/include/eunit.hrl").

-export([init/1,
         read/2,
         write/3,
         commit/2,
         rollback/2,
         add_ref/3,
         delete_ref/3,
         new_tid/0,
         commit_id/1,
         gen_seqs/1, collect/1
        ]).

-record(vs,
        { committed,         %% committed
          num_generations,
          version_count,
          oldest_gen,
          transactions,      %% pending transactions
          oldest_trans,
          max_generations = 4,
          objects            %% referenced
        }).

-record(gen,
        { earliest,
          latest,
          values,
          count,
          deleted_refs
        }).

-record(txn,
        { mode = none,
          at,
          value,
          added_refs = #{},
          deleted_refs = #{} }).

-record(tid, {t, umi, off}).

%% Because of the interface of gb_trees, we're forced to negate all our times,
%% which makes the usual comparison operators super confusing
-define(OLDEST,[oldest]). %% lists compare greater than tuples
-define(NEWEST,newest).   %% atoms compare less than tuples
-define(IS_OLDER(A,B), A > B).
-define(IS_NEWER(A,B), A < B).
-define(OLDER_OF(A,B), max(A,B)).
-define(NEWER_OF(A,B), min(A,B)).

init(Value) ->
    #vs{ committed = [ new_generation(?OLDEST, Value) ],
         num_generations = 1,
         version_count = 1,
         transactions = gb_trees:from_orddict([])
       }.

read(Tid, #vs{ transactions = Ts } = Store) ->
    case gb_trees:lookup(Tid, Ts) of
        {value, #txn{ mode = read, at = At }} ->
            {get_version(At, Store), Store};
        {value, #txn{ mode = write, value = Val }} ->
            {Val, Store};
        none ->
            {Val,At} = version_before(Tid, Store),
            Txn = #txn{ mode = read, at = At },
            Ts1 = gb_trees:insert(Tid, Txn, Ts),
            {Val, Store#vs{ transactions = Ts1 }}
    end.

write(Tid, Value, #vs{ transactions = Ts} = Store) ->
    Iter = gb_trees:iterator_from(Tid, Ts),
    case gb_trees:next(Iter) of
        {Tid, #txn{ mode = read } = Txn, Iter1} ->
            case gb_trees:next(Iter1) of
                none ->
                    Txn1 = Txn#txn{ mode = write, value = Value },
                    Ts1 = gb_trees:update(Tid, Txn1, Ts),
                    Store#vs{ transactions = Ts1 };
                {OlderTid, _, _} ->
                    error({non_serial, OlderTid})
            end;
        {Tid, #txn{ mode = write } = Txn, _Iter1} ->
            %% Once a transaction has written, it's allowed to re-write
            Txn1 = Txn#txn{ value = Value },
            Ts1 = gb_trees:update(Tid, Txn1, Ts),
            Store#vs{ transactions = Ts1 };
        {_OlderTid, _, _} ->
            %% If this transaction is just going to clobber the value
            %% it doesn't matter if an older transaction has read/written it
            Txn1 = #txn{ mode = write, value = Value },
            Ts1 = gb_trees:insert(Tid, Txn1, Ts),
            Store#vs{ transactions = Ts1}
    end.

commit(Tid, #vs{ transactions = Ts,
                 committed = [#gen{ latest = Latest }|_] } = Store) ->
    case gb_trees:lookup(Tid, Ts) of
        none ->
            Store;
        {value, #txn{ mode = read }} ->
            %% this transaction has only read the value
            Store1 = remove_tid(Tid, Store),
            Store1;
        {value, #txn{ mode = write, value = Val, at = At}}
          when At =:= Latest orelse At == undefined ->
            Store1 = add_version(commit_id(Tid), Val, Store),
            Store2 = remove_tid(Tid, Store1),
            Store2;
        {value, #txn{ mode = write, at = _NotLatest}} ->
            error({non_serial, Latest})
    end.

rollback(Tid, #vs{ transactions = Ts } = Store) ->
    case gb_trees:lookup(Tid, Ts) of
        none ->
            Store;
        _ ->
            remove_tid(Tid, Store)
    end.

add_ref(Tid, #vs{} = Object, #vs{ transactions = Ts,
                                  objects = Objects } = Store) ->
    Ref = make_ref(),
    Ts1 =
        case gb_trees:lookup(Tid, Ts) of
            {value, #txn{ added_refs = Added } = Txn} ->
                Added1 = sets:add_element(Ref, Added),
                gb_trees:update(Tid, Txn#txn{added_refs = Added1}, Ts);
            none ->
                Txn = #txn{ added_refs = set:from_list([Ref]) },
                gb_trees:insert(Tid, Txn, Ts)
        end,
    Objects1 = maps:put(Ref, Object, Objects),
    Store#vs{ transactions = Ts1,
                   objects = Objects1
            }.

delete_ref(Tid, Ref, #vs{ transactions = Ts } = Store) ->
    Ts1 =
        case gb_trees:lookup(Tid, Ts) of
            {value, #txn{ deleted_refs = Deleted } = Txn} ->
                Deleted1 = sets:add_element(Ref, Deleted),
                gb_trees:update(Tid, Txn#txn{deleted_refs = Deleted1}, Ts);
            none ->
                Txn = #txn{ deleted_refs = set:from_list([Ref]) },
                gb_trees:insert(Tid, Txn, Ts)
        end,
    Store#vs{ transactions = Ts1 }.

new_tid() ->
    #tid{ t = -1 * erlang:monotonic_time(),
          umi = -1 * erlang:unique_integer([monotonic]),
          off = erlang:time_offset()
        }.

commit_id(Tid) ->
    Tid#tid{ t = -1 * erlang:monotonic_time(),
             off = erlang:time_offset()
           }.


remove_tid(Tid, #vs{ transactions = Ts } = Store) ->
    Ts1 = gb_trees:delete(Tid, Ts),
    case gb_trees:is_empty(Ts1) of
        true -> Store#vs{ transactions = Ts1, oldest_trans = undefined };
        false ->
            {Oldest, _} = gb_trees:largest(Ts1),
            Store#vs{ transactions = Ts1, oldest_trans = Oldest }
    end.

add_version(Tid, Txn, #vs{} = Store) ->
    Store1 = #vs{ committed = [CurrentGen | OlderGens],
                  num_generations = NGen,
                  max_generations = MaxGen,
                  version_count = VCount
                } = gc(Store),
    #gen{ count = CurrentCount } = CurrentGen,
    if NGen < MaxGen andalso CurrentCount >= (VCount / NGen) ->
            NewGen = new_generation(Tid, Txn),
            Store1#vs{ committed = [NewGen, CurrentGen | OlderGens],
                       version_count = VCount + 1,
                       num_generations = NGen + 1 };
       true ->
            CurrentGen1 = add_to_generation(Tid, Txn, CurrentGen),
            Store1#vs{ committed = [CurrentGen1 | OlderGens],
                       version_count = VCount + 1 }
    end.

get_version(Tid, #vs{ committed = Generations }) ->
    get_version(Tid, Generations);
get_version(Tid, [#gen{ earliest = Earliest } | Generations]) when ?IS_NEWER(Earliest, Tid) ->
    get_version(Tid, Generations);
get_version(Tid, [#gen{ values = Values } | _]) ->
    gb_trees:get(Tid, Values).

version_before(Tid, #vs{ committed = Generations }) ->
    version_before(Tid, Generations);
version_before(Tid, [#gen{ earliest = Earliest } | Generations]) when ?IS_NEWER(Earliest, Tid) ->
    version_before(Tid, Generations);
version_before(Tid, [#gen{ values = Values } | Generations]) ->
    Iter = gb_trees:iterator_from(Tid, Values),
    case gb_trees:next(Iter) of
        {At, Val, _} ->
            {Val, At};
        none ->
            version_before(Tid, Generations)
    end;
version_before(_Tid, []) ->
    error(too_old).

new_generation(Tid, #txn{ value = Value, deleted_refs = DelRefs }) ->
    new_generation(Tid, Value, DelRefs);
new_generation(Tid, Value) ->
    new_generation(Tid, Value, sets:new()).

new_generation(Tid, Value, DelRefs) ->
    #gen{ earliest = Tid,
          latest = Tid,
          values = gb_trees:from_orddict([{Tid, Value}]),
          deleted_refs = DelRefs,
          count = 1 }.

add_to_generation(Tid, #txn{ value = Value,
                             deleted_refs = TxnDelRefs }, Generation) ->
    #gen{ latest = Latest,
          count = N,
          deleted_refs = GenDelRefs,
          values = Vals } = Generation,
    Vals1 = gb_trees:insert(Tid, Value, Vals),
    Generation#gen{ latest = ?NEWER_OF(Tid, Latest),
                    count = N + 1,
                    deleted_refs = sets:union(GenDelRefs, TxnDelRefs),
                    values = Vals1 }.

gc(#vs{ oldest_trans = OldestTrans,
        oldest_gen = OldestGen } = Store) when ?IS_NEWER(OldestGen, OldestTrans) ->
    Store;
gc(#vs{ committed = Generations,
        oldest_trans = OldestTrans,
        objects = Objects } = Store) ->
    {Generations1, OldestGeneration, NumGenerations, VersionCount, DeletedRefs}
        = clean_generations(OldestTrans, Generations),
    Objects1 = lists:foldl(fun maps:remove/2, Objects, DeletedRefs),
    Store#vs{ committed = Generations1,
              oldest_gen = OldestGeneration,
              num_generations = NumGenerations,
              version_count = VersionCount,
              objects = Objects1
            }.

clean_generations(First, [#gen{ latest = Latest, count = Count } =Current | Gens]) ->
    clean_generations(First, Gens, [Current], Latest, 1, Count).

clean_generations(_First, [], Out, OldestGen, NGens, NItems) ->
    {lists:reverse(Out), OldestGen, NGens, NItems, []};
clean_generations(First, [#gen{ latest = Latest } | Discarded],
                  Out, OldestGen, NGens, NItems)
  when ?IS_OLDER(Latest, First) ->
    DeletedRefs = lists:foldl(fun(#gen{ deleted_refs = GenDel }, Dels) ->
                                      sets:union(GenDel, Dels)
                              end, sets:new(), Discarded),
    {lists:reverse(Out), OldestGen, NGens, NItems, sets:to_list(DeletedRefs)};
clean_generations(First, [#gen{ latest = Latest, count = Count } = Gen | More],
                  Out, OldestGen, NGens, NItems) ->
    clean_generations(First, More, [Gen | Out],
                      ?OLDER_OF(Latest, OldestGen), NGens + 1, NItems + Count ).


transaction_test() ->
    Store1 = init(5),
    io:format("Store1: ~p~n",[Store1]),

    Trans1 = new_tid(),
    {5, Store2} = read(Trans1, Store1),
    io:format("Store2: ~p~n",[Store2]),

    Store3 = write(Trans1, 6, Store2),
    io:format("Store3: ~p~n",[Store3]),

    Trans2 = new_tid(),
    {5, Store4} = read(Trans2, Store3),
    io:format("Store4: ~p~n",[Store4]),

    Store5 = commit(Trans1, Store4),
    Trans3 = new_tid(),
    io:format("Store5: ~p~n",[Store5]),

    {6, _Store6} = read(Trans3, Store5).

transaction_seq_test_() ->
    %% Each transaction: reads the value, adds N, and commits it
    %% Regardless of order of execution, the final sum should be the same
    Seqs = gen_seqs({3,3,3,3}),
    %% Seqs = [[4,4,3,3,3,2,2,4,2,1,1,1]],
    io:format("testing ~p sequences~n",[Seqs]),
    Tests = [ {timeout, 1, fun() -> transaction_seq_test(Seq) end} 
              || Seq <- Seqs ],
    {inparallel, 500, Tests}.

transaction_seq_test(Seq) ->
    io:format("Seq: ~p~n",[Seq]),
    Store = init(0),
    Transactions = [ {N, new_tid(), read, undefined} || N <- lists:seq(1,4) ],
    transaction_seq_test(Store, Seq, Transactions).


transaction_seq_test(Store, [N|Ns], Ts) ->
    {Store1, Ts1} = perform_nth(Store, N, Ts),
    transaction_seq_test(Store1, Ns, Ts1);
transaction_seq_test(Store, _Ns, []) ->
    {10,_} = read(new_tid(), Store);
transaction_seq_test(_Store, [], Ts) ->
    {error, transactions_incomplete, Ts}.




perform_nth(Store, N, Ts) ->
    perform_nth(Store, N, Ts, []).

perform_nth(Store, 1, [T|Ts], Hd) ->
    %io:format("Performing ~p on:~n~p~n",[T,Store]),
    case perform(Store, T) of
        {Store1, complete} ->
            {Store1, lists:reverse(Hd) ++ Ts};
        {Store1, T1} ->
            {Store1, lists:reverse(Hd) ++ [T1|Ts]}
    end;
perform_nth(Store, N, [T|Ts], Hd) ->
    perform_nth(Store, N - 1, Ts, [T|Hd]);
perform_nth(Store, N, [], Hd) ->
    perform_nth(Store, N, lists:reverse(Hd), []).

perform(Store, {Add, Tid, read, undefined}) ->
    try read(Tid, Store) of
        {Val, Store1} ->
            io:format("Transaction ~p read: ~p~n",[Add,Val]),
            {Store1, {Add, Tid, write, Val}}
    catch
        error:too_old ->
            io:format("Transaction ~p too old~n",[Add]),
            {Store, {Add, new_tid(), read, undefined}}
    end;
perform(Store, {Add, Tid, write, Val}) ->
    try write(Tid, Val+Add, Store) of
        Store1 ->
            io:format("Transaction ~p wrote: ~p~n",
                      [Add,Val+Add]),
            {Store1, {Add, Tid, commit, Val}}
    catch
        error:{non_serial,_} ->
            io:format("Transaction ~p rolled back~n",[Add]),
            Store1 = rollback(Tid, Store),
            {Store1, {Add, new_tid(), read, undefined}}
    end;
perform(Store, {Add, Tid, commit, _}) ->
    try commit(Tid, Store) of
        Store1 ->
            io:format("Transaction ~p committed~n",[Add]),
            {Store1, complete}
    catch
        error:{non_serial,_} ->
            io:format("Transaction ~p rolled back~n",[Add]),
            Store1 = rollback(Tid, Store),
            {Store1, {Add, new_tid(), read, undefined}}
    end.

gen_seqs(Counts) ->
    CList = tuple_to_list(Counts),
    Len = lists:sum(CList),
    ByCount = collect(CList),
    lists:concat(
      [ gen_seqs(Len, Counts, [], N)
        || {_Count,[N|_]} <- ByCount ]).

gen_seqs(Len, Counts, Seq) ->
    lists:concat([ gen_seqs(Len, Counts, Seq, N)
                   || N <- lists:seq(1,size(Counts)) ]).

gen_seqs(0, _, Seq, _Pos) ->
    [lists:reverse(Seq)];
gen_seqs(_Len, Counts, _Seq, Pos) when element(Pos, Counts) < 1 ->
    [];
gen_seqs(Len, Counts, Seq, Pos) ->
    Counts1 = setelement(Pos, Counts, element(Pos,Counts) - 1),
    gen_seqs(Len - 1, Counts1, [Pos|Seq]).


collect(L) ->
    Collected =
        lists:foldl(fun({I,N},D) -> dict:append(N,I,D) end,
                    dict:new(),
                    lists:zip(lists:seq(1,length(L)),L)),
    dict:to_list(Collected).
