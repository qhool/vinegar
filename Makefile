include stdapp.mk

TEST_MODULES := $(notdir $(ERL_SOURCES:%.erl=%))

run-tests: tests
	erl -pa ebin -noshell -eval 'eunit:test(lists:map(fun erlang:list_to_atom/1,string:tokens("$(TEST_MODULES)"," "))).' -s init stop
