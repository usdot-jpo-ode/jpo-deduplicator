default:
	$(info Make target options:)
	$(info `make start` to run the deduplicator)
	$(info `make build` to build the deduplicator)
	$(info `make stop` to stop the deduplicator)
	$(info `make delete` to stop the deduplicator and remove the volumes)
	$(info `make restart` to stop and then start the deduplicator)
	$(info `make rebuild` to stop, delete, and then rebuild the containers)
	$(info `make clean-build` to rebuild the containers without using the cache)

.PHONY: start
start:
ifeq ("$(wildcard .env)", "")
	$(error "ERROR: deduplicator Environment file `.env` not found in ${PWD}")
endif
ifeq ("$(wildcard ./jpo-utils/.env)", "")
	$(error "ERROR: jpo-utils Environment file `.env` not found in ${PWD}")
endif
	docker compose up -d

build:
ifeq ("$(wildcard .env)", "")
	$(error "ERROR: jpo-ode Environment file `.env` not found in ${PWD}")
endif
ifeq ("$(wildcard ./jpo-utils/.env)", "")
	$(error "ERROR: jpo-utils Environment file `.env` not found in ${PWD}")
endif
	docker compose build

.PHONY: stop
stop:
	docker compose down

.PHONY: delete
delete:
	docker compose down -v

.PHONY: restart
restart:
	$(MAKE) stop start

.PHONY: rebuild
rebuild:
	$(MAKE) stop delete build start

.PHONY: clean-build
clean-build:
	docker compose build --no-cache