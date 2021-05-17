BASEDIR = $(shell pwd)
REBAR = $(BASEDIR)/rebar3
BASIC_PROFILE = default
DEV_PROFILE = default
RELPATH = _build/$(BASIC_PROFILE)/rel/rb_sequencer
DEV1RELPATH = _build/$(DEV_PROFILE)/rel/rb_sequencer_local1
DEV2RELPATH = _build/$(DEV_PROFILE)/rel/rb_sequencer_local2
DEV3RELPATH = _build/$(DEV_PROFILE)/rel/rb_sequencer_local3
DEV4RELPATH = _build/$(DEV_PROFILE)/rel/rb_sequencer_local4
APPNAME = rb_sequencer
ENVFILE = env
SHELL = /bin/bash

.PHONY: test

all: compile

compile:
	$(REBAR) as $(BASIC_PROFILE) compile

check_binaries:
	$(REBAR) as debug_bin compile

xref:
	- $(REBAR) xref skip_deps=true

dialyzer:
	- $(REBAR) dialyzer

debug:
	$(REBAR) as debug_log compile

clean:
	$(REBAR) clean --all

rel: compile
	$(REBAR) as $(BASIC_PROFILE) release -n rb_sequencer

debugrel:
	$(REBAR) as debug_log release -n rb_sequencer

debugrel-clean:
	rm -rf _build/debug_log/rel

console:
	$(BASEDIR)/$(RELPATH)/bin/$(ENVFILE) console

relclean:
	rm -rf $(BASEDIR)/$(RELPATH)

start:
	$(BASEDIR)/$(RELPATH)/bin/$(ENVFILE) daemon

ping:
	$(BASEDIR)/$(RELPATH)/bin/$(ENVFILE) ping

stop:
	$(BASEDIR)/$(RELPATH)/bin/$(ENVFILE) stop

attach:
	$(BASEDIR)/$(RELPATH)/bin/$(ENVFILE) daemon_attach

test:
	${REBAR} eunit skip_deps=true

ct:
	$(REBAR) as $(BASIC_PROFILE) ct --fail_fast

ct_clean:
	rm -rf $(BASEDIR)/_build/test/logs

dev1-rel:
	$(REBAR) as $(DEV_PROFILE) release -n rb_sequencer_local1

dev2-rel:
	$(REBAR) as $(DEV_PROFILE) release -n rb_sequencer_local2

dev3-rel:
	$(REBAR) as $(DEV_PROFILE) release -n rb_sequencer_local3

dev4-rel:
	$(REBAR) as $(DEV_PROFILE) release -n rb_sequencer_local4

devrel: dev1-rel dev2-rel dev3-rel dev4-rel

dev1-attach:
	INSTANCE_NAME=rb_sequencer_local1 $(BASEDIR)/$(DEV1RELPATH)/bin/$(ENVFILE) daemon_attach

dev2-attach:
	INSTANCE_NAME=rb_sequencer_local2 $(BASEDIR)/$(DEV2RELPATH)/bin/$(ENVFILE) daemon_attach

dev3-attach:
	INSTANCE_NAME=rb_sequencer_local3 $(BASEDIR)/$(DEV3RELPATH)/bin/$(ENVFILE) daemon_attach

dev4-attach:
	INSTANCE_NAME=rb_sequencer_local4 $(BASEDIR)/$(DEV4RELPATH)/bin/$(ENVFILE) daemon_attach

devrelclean:
	rm -rf _build/$(DEV_PROFILE)/rel/rb_sequencer_local*

dev1-start:
	RIAK_RING_SIZE=$(DEVRINGSIZE) INSTANCE_NAME=rb_sequencer_local1 TCP_LIST_PORT=7891 INTER_DC_PORT=8991 $(BASEDIR)/$(DEV1RELPATH)/bin/$(ENVFILE) daemon

dev2-start:
	RIAK_RING_SIZE=$(DEVRINGSIZE) INSTANCE_NAME=rb_sequencer_local2 TCP_LIST_PORT=7892 INTER_DC_PORT=8992 $(BASEDIR)/$(DEV2RELPATH)/bin/$(ENVFILE) daemon

dev3-start:
	RIAK_RING_SIZE=$(DEVRINGSIZE) INSTANCE_NAME=rb_sequencer_local3 TCP_LIST_PORT=7893 INTER_DC_PORT=8993 $(BASEDIR)/$(DEV3RELPATH)/bin/$(ENVFILE) daemon

dev4-start:
	RIAK_RING_SIZE=$(DEVRINGSIZE) INSTANCE_NAME=rb_sequencer_local4 TCP_LIST_PORT=7894 INTER_DC_PORT=8994 $(BASEDIR)/$(DEV4RELPATH)/bin/$(ENVFILE) daemon

devstart: clean devrelclean devrel dev1-start dev2-start dev3-start dev4-start

dev1-ping:
	INSTANCE_NAME=rb_sequencer_local1 $(BASEDIR)/$(DEV1RELPATH)/bin/$(ENVFILE) ping

dev2-ping:
	INSTANCE_NAME=rb_sequencer_local2 $(BASEDIR)/$(DEV2RELPATH)/bin/$(ENVFILE) ping

dev3-ping:
	INSTANCE_NAME=rb_sequencer_local3 $(BASEDIR)/$(DEV3RELPATH)/bin/$(ENVFILE) ping

dev4-ping:
	INSTANCE_NAME=rb_sequencer_local4 $(BASEDIR)/$(DEV4RELPATH)/bin/$(ENVFILE) ping

devping: dev1-ping dev2-ping dev3-ping dev4-ping

dev1-stop:
	INSTANCE_NAME=rb_sequencer_local1 $(BASEDIR)/$(DEV1RELPATH)/bin/$(ENVFILE) stop

dev2-stop:
	INSTANCE_NAME=rb_sequencer_local2 $(BASEDIR)/$(DEV2RELPATH)/bin/$(ENVFILE) stop

dev3-stop:
	INSTANCE_NAME=rb_sequencer_local3 $(BASEDIR)/$(DEV3RELPATH)/bin/$(ENVFILE) stop

dev4-stop:
	INSTANCE_NAME=rb_sequencer_local4 $(BASEDIR)/$(DEV4RELPATH)/bin/$(ENVFILE) stop

devstop: dev1-stop dev2-stop dev3-stop dev4-stop

devreplicas:
	./bin/connect_dcs.erl -l local1 'local1:127.0.0.1:8991' 'local2:127.0.0.1:8992' 'local3:127.0.0.1:8993' 'local4:127.0.0.1:8994'
