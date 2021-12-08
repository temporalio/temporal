############################# Main targets #############################
# Install all tools and builds binaries.
install: update-tools bins

# Rebuild binaries (used by Dockerfile).
bins: clean-bins temporal-server tctl plugins temporal-cassandra-tool temporal-sql-tool

# Install all tools, recompile proto files, run all possible checks and tests (long but comprehensive).
all: update-tools clean proto bins check test

# Used by Buildkite.
ci-build: bins build-tests update-tools shell-check check proto mocks gomodtidy ensure-no-changes

# Delete all build artefacts.
clean: clean-bins clean-test-results

# Recompile proto files.
proto: clean-proto install-proto-submodule buf-lint api-linter protoc fix-proto-path goimports-proto proto-mocks copyright-proto

# Update proto submodule from remote and recompile proto files.
update-proto: clean-proto update-proto-submodule buf-lint api-linter protoc fix-proto-path update-go-api goimports-proto proto-mocks copyright-proto gomodtidy
########################################################################

.PHONY: proto

##### Arguments ######

GOOS ?= $(shell go env GOOS)
GOARCH ?= $(shell go env GOARCH)
GOPATH ?= $(shell go env GOPATH)

PERSISTENCE_TYPE ?= nosql
PERSISTENCE_DRIVER ?= cassandra

# Optional args to create multiple keyspaces:
# make install-schema TEMPORAL_DB=temporal2 VISIBILITY_DB=temporal_visibility2
TEMPORAL_DB ?= temporal
VISIBILITY_DB ?= temporal_visibility

DOCKER_IMAGE_TAG ?= test
# Pass "registry" to automatically push image to the docker hub.
DOCKER_BUILDX_OUTPUT ?= image

# Name resolution requires cgo to be enabled on macOS and Windows: https://golang.org/pkg/net/#hdr-Name_Resolution.
ifndef CGO_ENABLED
	ifeq ($(GOOS),linux)
	CGO_ENABLED := 0
	else
	CGO_ENABLED := 1
	endif
endif

ifdef TEST_TAG
override TEST_TAG := -tags $(TEST_TAG)
endif

##### Variables ######

GOBIN := $(if $(shell go env GOBIN),$(shell go env GOBIN),$(GOPATH)/bin)
PATH := $(GOBIN):$(PATH)

MODULE_ROOT := $(lastword $(shell grep -e "^module " go.mod))
COLOR := "\e[1;36m%s\e[0m\n"
RED :=   "\e[1;31m%s\e[0m\n"

define NEWLINE


endef

TEST_TIMEOUT := 20m

INTEG_TEST_ROOT        := ./host
INTEG_TEST_XDC_ROOT    := ./host/xdc
INTEG_TEST_NDC_ROOT    := ./host/ndc

PROTO_ROOT := proto
PROTO_FILES = $(shell find ./$(PROTO_ROOT)/internal -name "*.proto")
PROTO_DIRS = $(sort $(dir $(PROTO_FILES)))
PROTO_IMPORTS := -I=$(PROTO_ROOT)/internal -I=$(PROTO_ROOT)/api -I=$(GOPATH)/src/github.com/temporalio/gogo-protobuf/protobuf
PROTO_OUT := api

ALL_SRC         := $(shell find . -name "*.go" | grep -v -e "^$(PROTO_OUT)")
TEST_DIRS       := $(sort $(dir $(filter %_test.go,$(ALL_SRC))))
INTEG_TEST_DIRS := $(filter $(INTEG_TEST_ROOT)/ $(INTEG_TEST_NDC_ROOT)/,$(TEST_DIRS))
UNIT_TEST_DIRS  := $(filter-out $(INTEG_TEST_ROOT)% $(INTEG_TEST_XDC_ROOT)% $(INTEG_TEST_NDC_ROOT)%,$(TEST_DIRS))

ALL_SCRIPTS     := $(shell find . -name "*.sh")

PINNED_DEPENDENCIES := \
	github.com/DataDog/sketches-go@v0.0.1 \
	github.com/apache/thrift@v0.0.0-20161221203622-b2a4d4ae21c7 \
	github.com/go-sql-driver/mysql@v1.5.0

# Code coverage output files.
COVER_ROOT                 := ./.coverage
UNIT_COVER_PROFILE         := $(COVER_ROOT)/unit_coverprofile.out
INTEG_COVER_PROFILE        := $(COVER_ROOT)/integ_$(PERSISTENCE_DRIVER)_coverprofile.out
INTEG_XDC_COVER_PROFILE    := $(COVER_ROOT)/integ_xdc_$(PERSISTENCE_DRIVER)_coverprofile.out
INTEG_NDC_COVER_PROFILE    := $(COVER_ROOT)/integ_ndc_$(PERSISTENCE_DRIVER)_coverprofile.out
SUMMARY_COVER_PROFILE      := $(COVER_ROOT)/summary.out

# Need the following option to have integration tests count towards coverage. godoc below:
# -coverpkg pkg1,pkg2,pkg3
#   Apply coverage analysis in each test to the given list of packages.
#   The default is for each test to analyze only the package being tested.
#   Packages are specified as import paths.
INTEG_TEST_COVERPKG := -coverpkg="$(MODULE_ROOT)/client/...,$(MODULE_ROOT)/common/...,$(MODULE_ROOT)/service/..."

##### Tools #####
update-checkers:
	@printf $(COLOR) "Install/update check tools..."
	@go install golang.org/x/lint/golint@latest # golint doesn't have releases.
	@go install golang.org/x/tools/cmd/goimports@v0.1.5
	@go install honnef.co/go/tools/cmd/staticcheck@v0.2.1
	@go install github.com/kisielk/errcheck@v1.6.0
	@go install github.com/googleapis/api-linter/cmd/api-linter@v1.27.0
	@go install github.com/bufbuild/buf/cmd/buf@v0.54.1

update-mockgen:
	@printf $(COLOR) "Install/update mockgen tool..."
	@go install github.com/golang/mock/mockgen@v1.6.0

update-proto-plugins:
	@printf $(COLOR) "Install/update proto plugins..."
	@go install github.com/temporalio/gogo-protobuf/protoc-gen-gogoslick@latest
# This to download sources of gogo-protobuf which are required to build proto files.
	@GO111MODULE=off go get github.com/temporalio/gogo-protobuf/protoc-gen-gogoslick
	@go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest

update-tools: update-checkers update-mockgen update-proto-plugins

##### Proto #####
$(PROTO_OUT):
	@mkdir -p $(PROTO_OUT)

clean-proto:
	@rm -rf $(PROTO_OUT)/*

update-proto-submodule:
	@printf $(COLOR) "Update proto submodule from remote..."
	git submodule update --force --remote $(PROTO_ROOT)/api

install-proto-submodule:
	@printf $(COLOR) "Install proto submodule..."
	git submodule update --init $(PROTO_ROOT)/api

protoc: $(PROTO_OUT)
	@printf $(COLOR) "Build proto files..."
# Run protoc separately for each directory because of different package names.
	$(foreach PROTO_DIR,$(PROTO_DIRS),\
		protoc $(PROTO_IMPORTS) \
		 	--gogoslick_out=Mgoogle/protobuf/descriptor.proto=github.com/golang/protobuf/protoc-gen-go/descriptor,Mgoogle/protobuf/duration.proto=github.com/gogo/protobuf/types,Mgoogle/protobuf/wrappers.proto=github.com/gogo/protobuf/types,Mgoogle/protobuf/timestamp.proto=github.com/gogo/protobuf/types,plugins=grpc,paths=source_relative:$(PROTO_OUT) \
			$(PROTO_DIR)*.proto \
	$(NEWLINE))

fix-proto-path:
	mv -f $(PROTO_OUT)/temporal/server/api/* $(PROTO_OUT) && rm -rf $(PROTO_OUT)/temporal

# All gRPC generated service files pathes relative to PROTO_OUT.
PROTO_GRPC_SERVICES = $(patsubst $(PROTO_OUT)/%,%,$(shell find $(PROTO_OUT) -name "service.pb.go"))
service_name = $(firstword $(subst /, ,$(1)))
mock_file_name = $(call service_name,$(1))mock/$(subst $(call service_name,$(1))/,,$(1:go=mock.go))

proto-mocks: $(PROTO_OUT)
	@printf $(COLOR) "Generate proto mocks..."
	$(foreach PROTO_GRPC_SERVICE,$(PROTO_GRPC_SERVICES),\
		cd $(PROTO_OUT) && \
		mockgen -package $(call service_name,$(PROTO_GRPC_SERVICE))mock -source $(PROTO_GRPC_SERVICE) -destination $(call mock_file_name,$(PROTO_GRPC_SERVICE)) \
	$(NEWLINE))

update-go-api:
	@printf $(COLOR) "Update go.temporal.io/api@master..."
	@go get -u go.temporal.io/api@master

goimports-proto:
	@printf $(COLOR) "Run goimports for proto files..."
	@goimports -w $(PROTO_OUT)

copyright-proto:
	@printf $(COLOR) "Update license headers for proto files..."
	@go run ./cmd/tools/copyright/licensegen.go --scanDir $(PROTO_OUT)

##### Binaries #####
clean-bins:
	@printf $(COLOR) "Delete old binaries..."
	@rm -f tctl
	@rm -f tctl-authorization-plugin
	@rm -f temporal-server
	@rm -f temporal-cassandra-tool
	@rm -f temporal-sql-tool

temporal-server:
	@printf $(COLOR) "Build temporal-server with OS: $(GOOS), ARCH: $(GOARCH)..."
	@./develop/scripts/create_build_info_data.sh
	CGO_ENABLED=$(CGO_ENABLED) go build -o temporal-server cmd/server/main.go

tctl:
	@printf $(COLOR) "Build tctl with OS: $(GOOS), ARCH: $(GOARCH)..."
	CGO_ENABLED=$(CGO_ENABLED) go build -o tctl cmd/tools/cli/main.go

plugins:
	@printf $(COLOR) "Build tctl-authorization-plugin with OS: $(GOOS), ARCH: $(GOARCH)..."
	CGO_ENABLED=$(CGO_ENABLED) go build -o tctl-authorization-plugin cmd/tools/cli/plugins/authorization/main.go

temporal-cassandra-tool:
	@printf $(COLOR) "Build temporal-cassandra-tool with OS: $(GOOS), ARCH: $(GOARCH)..."
	CGO_ENABLED=$(CGO_ENABLED) go build -o temporal-cassandra-tool cmd/tools/cassandra/main.go

temporal-sql-tool:
	@printf $(COLOR) "Build temporal-sql-tool with OS: $(GOOS), ARCH: $(GOARCH)..."
	CGO_ENABLED=$(CGO_ENABLED) go build -o temporal-sql-tool cmd/tools/sql/main.go

##### Checks #####
copyright-check:
	@printf $(COLOR) "Check license header..."
	@go run ./cmd/tools/copyright/licensegen.go --verifyOnly

copyright:
	@printf $(COLOR) "Fix license header..."
	@go run ./cmd/tools/copyright/licensegen.go

lint:
	@printf $(COLOR) "Run linter..."
	@golint ./...

vet:
	@printf $(COLOR) "Run go vet..."
	@go vet ./... || true

goimports-check:
	@printf $(COLOR) "Run goimports checks..."
	@GO_IMPORTS_OUTPUT=$$(goimports -l .); if [ -n "$${GO_IMPORTS_OUTPUT}" ]; then echo "$${GO_IMPORTS_OUTPUT}" && echo "Please run make goimports" && exit 1; fi

goimports:
	@printf $(COLOR) "Run goimports..."
	@goimports -w .

staticcheck:
	@printf $(COLOR) "Run staticcheck..."
	@staticcheck -fail none ./...

errcheck:
	@printf $(COLOR) "Run errcheck..."
	@errcheck ./... || true

api-linter:
	@printf $(COLOR) "Run api-linter..."
	@api-linter --set-exit-status $(PROTO_IMPORTS) --config=$(PROTO_ROOT)/api-linter.yaml $(PROTO_FILES)

buf-lint:
	@printf $(COLOR) "Run buf linter..."
	@(cd $(PROTO_ROOT) && buf lint)

buf-build:
	@printf $(COLOR) "Build image.bin with buf..."
	@(cd $(PROTO_ROOT) && buf build -o image.bin)

buf-breaking:
	@printf $(COLOR) "Run buf breaking changes check against image.bin..."
	@(cd $(PROTO_ROOT) && buf check breaking --against image.bin)

shell-check:
	@printf $(COLOR) "Run shellcheck for script files..."
	@shellcheck $(ALL_SCRIPTS)

check: copyright-check goimports-check lint vet staticcheck errcheck

##### Tests #####
clean-test-results:
	@rm -f test.log
	@go clean -testcache

build-tests:
	@printf $(COLOR) "Build tests..."
	@go test -exec="true" -count=0 -tags=esintegration $(TEST_DIRS)

unit-test: clean-test-results
	@printf $(COLOR) "Run unit tests..."
	$(foreach UNIT_TEST_DIR,$(UNIT_TEST_DIRS),\
		@go test $(UNIT_TEST_DIR) -timeout=$(TEST_TIMEOUT) $(TEST_TAG) -race | tee -a test.log \
	$(NEWLINE))
	@! grep -q "^--- FAIL" test.log

integration-test: clean-test-results
	@printf $(COLOR) "Run integration tests..."
	$(foreach INTEG_TEST_DIR,$(INTEG_TEST_DIRS),\
		@go test $(INTEG_TEST_DIR) -timeout=$(TEST_TIMEOUT) $(TEST_TAG) -race | tee -a test.log \
	$(NEWLINE))
# Need to run xdc tests with race detector off because of ringpop bug causing data race issue.
	@go test $(INTEG_TEST_XDC_ROOT) -timeout=$(TEST_TIMEOUT) $(TEST_TAG) | tee -a test.log
	@! grep -q "^--- FAIL" test.log

integration-with-fault-injection-test: clean-test-results
	@printf $(COLOR) "Run integration tests..."
	$(foreach INTEG_TEST_DIR,$(INTEG_TEST_DIRS),\
		@go test $(INTEG_TEST_DIR) -timeout=$(TEST_TIMEOUT) $(TEST_TAG) -race  -PersistenceFaultInjectionRate=0.005 | tee -a test.log \
	$(NEWLINE))
# Need to run xdc tests with race detector off because of ringpop bug causing data race issue.
	@go test $(INTEG_TEST_XDC_ROOT) -timeout=$(TEST_TIMEOUT) $(TEST_TAG) -PersistenceFaultInjectionRate=0.005 | tee -a test.log
	@! grep -q "^--- FAIL" test.log


test: unit-test integration-test integration-with-fault-injection-test

##### Coverage #####
$(COVER_ROOT):
	@mkdir -p $(COVER_ROOT)

unit-test-coverage: $(COVER_ROOT)
	@printf $(COLOR) "Run unit tests with coverage..."
	@echo "mode: atomic" > $(UNIT_COVER_PROFILE)
	$(foreach UNIT_TEST_DIR,$(patsubst ./%/,%,$(UNIT_TEST_DIRS)),\
		@mkdir -p $(COVER_ROOT)/$(UNIT_TEST_DIR); \
		go test ./$(UNIT_TEST_DIR) -timeout=$(TEST_TIMEOUT) -race -coverprofile=$(COVER_ROOT)/$(UNIT_TEST_DIR)/coverprofile.out || exit 1; \
		grep -v -e "^mode: \w\+" $(COVER_ROOT)/$(UNIT_TEST_DIR)/coverprofile.out >> $(UNIT_COVER_PROFILE) || true \
	$(NEWLINE))

integration-test-coverage: $(COVER_ROOT)
	@printf $(COLOR) "Run integration tests with coverage with $(PERSISTENCE_DRIVER) driver..."
	@go test $(INTEG_TEST_ROOT) -timeout=$(TEST_TIMEOUT) -race $(TEST_TAG) -persistenceType=$(PERSISTENCE_TYPE) -persistenceDriver=$(PERSISTENCE_DRIVER) $(INTEG_TEST_COVERPKG) -coverprofile=$(INTEG_COVER_PROFILE)

integration-test-xdc-coverage: $(COVER_ROOT)
	@printf $(COLOR) "Run integration test for cross DC with coverage with $(PERSISTENCE_DRIVER) driver..."
	@go test $(INTEG_TEST_XDC_ROOT) -timeout=$(TEST_TIMEOUT) $(TEST_TAG) -persistenceType=$(PERSISTENCE_TYPE) -persistenceDriver=$(PERSISTENCE_DRIVER) $(INTEG_TEST_COVERPKG) -coverprofile=$(INTEG_XDC_COVER_PROFILE)

integration-test-ndc-coverage: $(COVER_ROOT)
	@printf $(COLOR) "Run integration test for NDC with coverage with $(PERSISTENCE_DRIVER) driver..."
	@go test $(INTEG_TEST_NDC_ROOT) -timeout=$(TEST_TIMEOUT) -race $(TEST_TAG) -persistenceType=$(PERSISTENCE_TYPE) -persistenceDriver=$(PERSISTENCE_DRIVER) $(INTEG_TEST_COVERPKG) -coverprofile=$(INTEG_NDC_COVER_PROFILE)

.PHONY: $(SUMMARY_COVER_PROFILE)
$(SUMMARY_COVER_PROFILE): $(COVER_ROOT)
	@printf $(COLOR) "Combine coverage reports to $(SUMMARY_COVER_PROFILE)..."
	@rm -f $(SUMMARY_COVER_PROFILE)
	@echo "mode: atomic" > $(SUMMARY_COVER_PROFILE)
	$(foreach COVER_PROFILE,$(wildcard $(COVER_ROOT)/*_coverprofile.out),\
		@printf "Add %s...\n" $(COVER_PROFILE); \
		grep -v -e "[Mm]ocks\?.go" -e "^mode: \w\+" $(COVER_PROFILE) >> $(SUMMARY_COVER_PROFILE) || true \
	$(NEWLINE))

coverage-report: $(SUMMARY_COVER_PROFILE)
	@printf $(COLOR) "Generate HTML report from $(SUMMARY_COVER_PROFILE) to $(SUMMARY_COVER_PROFILE).html..."
	@go tool cover -html=$(SUMMARY_COVER_PROFILE) -o $(SUMMARY_COVER_PROFILE).html

ci-coverage-report: $(SUMMARY_COVER_PROFILE) coverage-report
	@printf $(COLOR) "Generate Coveralls report from $(SUMMARY_COVER_PROFILE)..."
	go install github.com/mattn/goveralls@v0.0.7
	@goveralls -coverprofile=$(SUMMARY_COVER_PROFILE) -service=buildkite || true

##### Schema #####
install-schema: temporal-cassandra-tool
	@printf $(COLOR) "Install Cassandra schema..."
	./temporal-cassandra-tool drop -k $(TEMPORAL_DB) -f
	./temporal-cassandra-tool create -k $(TEMPORAL_DB) --rf 1
	./temporal-cassandra-tool -k $(TEMPORAL_DB) setup-schema -v 0.0
	./temporal-cassandra-tool -k $(TEMPORAL_DB) update-schema -d ./schema/cassandra/temporal/versioned
	./temporal-cassandra-tool drop -k $(VISIBILITY_DB) -f
	./temporal-cassandra-tool create -k $(VISIBILITY_DB) --rf 1
	./temporal-cassandra-tool -k $(VISIBILITY_DB) setup-schema -v 0.0
	./temporal-cassandra-tool -k $(VISIBILITY_DB) update-schema -d ./schema/cassandra/visibility/versioned

install-schema-mysql: temporal-sql-tool
	@printf $(COLOR) "Install MySQL schema..."
	./temporal-sql-tool -u temporal --pw temporal drop --db $(TEMPORAL_DB) -f
	./temporal-sql-tool -u temporal --pw temporal create --db $(TEMPORAL_DB)
	./temporal-sql-tool -u temporal --pw temporal --db $(TEMPORAL_DB) setup-schema -v 0.0
	./temporal-sql-tool -u temporal --pw temporal --db $(TEMPORAL_DB) update-schema -d ./schema/mysql/v57/temporal/versioned
	./temporal-sql-tool -u temporal --pw temporal drop --db $(VISIBILITY_DB) -f
	./temporal-sql-tool -u temporal --pw temporal create --db $(VISIBILITY_DB)
	./temporal-sql-tool -u temporal --pw temporal --db $(VISIBILITY_DB) setup-schema -v 0.0
	./temporal-sql-tool -u temporal --pw temporal --db $(VISIBILITY_DB) update-schema -d ./schema/mysql/v57/visibility/versioned

install-schema-postgresql: temporal-sql-tool
	@printf $(COLOR) "Install Postgres schema..."
	./temporal-sql-tool -u temporal -pw temporal -p 5432 --pl postgres drop --db $(TEMPORAL_DB) -f
	./temporal-sql-tool -u temporal -pw temporal -p 5432 --pl postgres create --db $(TEMPORAL_DB)
	./temporal-sql-tool -u temporal -pw temporal -p 5432 --pl postgres --db $(TEMPORAL_DB) setup -v 0.0
	./temporal-sql-tool -u temporal -pw temporal -p 5432 --pl postgres --db $(TEMPORAL_DB) update-schema -d ./schema/postgresql/v96/temporal/versioned
	./temporal-sql-tool -u temporal -pw temporal -p 5432 --pl postgres drop --db $(VISIBILITY_DB) -f
	./temporal-sql-tool -u temporal -pw temporal -p 5432 --pl postgres create --db $(VISIBILITY_DB)
	./temporal-sql-tool -u temporal -pw temporal -p 5432 --pl postgres --db $(VISIBILITY_DB) setup-schema -v 0.0
	./temporal-sql-tool -u temporal -pw temporal -p 5432 --pl postgres --db $(VISIBILITY_DB) update-schema -d ./schema/postgresql/v96/visibility/versioned

install-schema-es:
	@printf $(COLOR) "Install Elasticsearch schema..."
	curl --fail -X PUT "http://127.0.0.1:9200/_cluster/settings" -H "Content-Type: application/json" --data-binary @./schema/elasticsearch/visibility/cluster_settings_v7.json --write-out "\n"
	curl --fail -X PUT "http://127.0.0.1:9200/_template/temporal_visibility_v1_template" -H "Content-Type: application/json" --data-binary @./schema/elasticsearch/visibility/index_template_v7.json --write-out "\n"
# No --fail here because create index is not idempotent operation.
	curl -X PUT "http://127.0.0.1:9200/temporal_visibility_v1_dev" --write-out "\n"

install-schema-cdc: temporal-cassandra-tool
	@printf $(COLOR)  "Set up temporal_active key space..."
	./temporal-cassandra-tool drop -k temporal_active -f
	./temporal-cassandra-tool create -k temporal_active --rf 1
	./temporal-cassandra-tool -k temporal_active setup-schema -v 0.0
	./temporal-cassandra-tool -k temporal_active update-schema -d ./schema/cassandra/temporal/versioned
	./temporal-cassandra-tool drop -k temporal_visibility_active -f
	./temporal-cassandra-tool create -k temporal_visibility_active --rf 1
	./temporal-cassandra-tool -k temporal_visibility_active setup-schema -v 0.0
	./temporal-cassandra-tool -k temporal_visibility_active update-schema -d ./schema/cassandra/visibility/versioned

	@printf $(COLOR) "Set up temporal_standby key space..."
	./temporal-cassandra-tool drop -k temporal_standby -f
	./temporal-cassandra-tool create -k temporal_standby --rf 1
	./temporal-cassandra-tool -k temporal_standby setup-schema -v 0.0
	./temporal-cassandra-tool -k temporal_standby update-schema -d ./schema/cassandra/temporal/versioned
	./temporal-cassandra-tool drop -k temporal_visibility_standby -f
	./temporal-cassandra-tool create -k temporal_visibility_standby --rf 1
	./temporal-cassandra-tool -k temporal_visibility_standby setup-schema -v 0.0
	./temporal-cassandra-tool -k temporal_visibility_standby update-schema -d ./schema/cassandra/visibility/versioned

##### Run server #####
start-dependencies:
	docker-compose -f ./develop/docker-compose/docker-compose.yml -f ./develop/docker-compose/docker-compose.$(GOOS).yml up

stop-dependencies:
	docker-compose -f ./develop/docker-compose/docker-compose.yml -f ./develop/docker-compose/docker-compose.$(GOOS).yml down

start: temporal-server
	./temporal-server start

start-es: temporal-server
	./temporal-server --zone es start

start-mysql: temporal-server
	./temporal-server --zone mysql start

start-mysql-es: temporal-server
	./temporal-server --zone mysql-es start

start-cdc-active: temporal-server
	./temporal-server --zone active start

start-cdc-standby: temporal-server
	./temporal-server --zone standby start

start-cdc-other: temporal-server
	./temporal-server --zone other start

##### Mocks #####
AWS_SDK_VERSION := $(lastword $(shell grep "github.com/aws/aws-sdk-go" go.mod))
external-mocks:
	@printf $(COLOR) "Generate external libraries mocks..."
	@mockgen -copyright_file ./LICENSE -package mocks -source $(GOPATH)/pkg/mod/github.com/aws/aws-sdk-go@$(AWS_SDK_VERSION)/service/s3/s3iface/interface.go | grep -v -e "^// Source: .*" > common/archiver/s3store/mocks/S3API.go

go-generate:
	@printf $(COLOR) "Process go:generate directives..."
	@go generate ./...

mocks: go-generate external-mocks

##### Fossa #####
fossa-install:
	curl -H 'Cache-Control: no-cache' https://raw.githubusercontent.com/fossas/fossa-cli/master/install.sh | bash

fossa-analyze:
	fossa analyze -b $${BUILDKITE_BRANCH:-$$(git branch --show-current)}

fossa-delay:
	echo "Fossa requested to add delay between analyze and test due to API race condition"
	sleep 30
	echo "Fossa wait complete"

fossa-test:
	fossa test --timeout 1800

build-fossa: bins fossa-analyze fossa-delay fossa-test

##### Docker #####
docker-server:
	@printf $(COLOR) "Building docker image temporalio/server:$(DOCKER_IMAGE_TAG)..."
	docker build . -t temporalio/server:$(DOCKER_IMAGE_TAG) --target temporal-server

docker-auto-setup:
	@printf $(COLOR) "Build docker image temporalio/auto-setup:$(DOCKER_IMAGE_TAG)..."
	docker build . -t temporalio/auto-setup:$(DOCKER_IMAGE_TAG) --target temporal-auto-setup

docker-tctl:
	@printf $(COLOR) "Build docker image temporalio/tctl:$(DOCKER_IMAGE_TAG)..."
	docker build . -t temporalio/tctl:$(DOCKER_IMAGE_TAG) --target temporal-tctl

docker-admin-tools:
	@printf $(COLOR) "Build docker image temporalio/admin-tools:$(DOCKER_IMAGE_TAG)..."
	docker build . -t temporalio/admin-tools:$(DOCKER_IMAGE_TAG) --target temporal-admin-tools

docker-buildx-container:
	docker buildx create --name builder-x --driver docker-container --use

docker-server-x:
	@printf $(COLOR) "Building cross-platform docker image temporalio/server:$(DOCKER_IMAGE_TAG)..."
	docker buildx build . -t temporalio/server:$(DOCKER_IMAGE_TAG) --platform linux/amd64,linux/arm64 --output type=$(DOCKER_BUILDX_OUTPUT) --target temporal-server

docker-auto-setup-x:
	@printf $(COLOR) "Build cross-platform docker image temporalio/auto-setup:$(DOCKER_IMAGE_TAG)..."
	docker buildx build . -t temporalio/auto-setup:$(DOCKER_IMAGE_TAG) --platform linux/amd64,linux/arm64 --output type=$(DOCKER_BUILDX_OUTPUT) --target temporal-auto-setup

docker-tctl-x:
	@printf $(COLOR) "Build cross-platform docker image temporalio/tctl:$(DOCKER_IMAGE_TAG)..."
	docker buildx build . -t temporalio/tctl:$(DOCKER_IMAGE_TAG) --platform linux/amd64,linux/arm64 --output type=$(DOCKER_BUILDX_OUTPUT) --target temporal-tctl

docker-admin-tools-x:
	@printf $(COLOR) "Build cross-platform docker image temporalio/admin-tools:$(DOCKER_IMAGE_TAG)..."
	docker buildx build . -t temporalio/admin-tools:$(DOCKER_IMAGE_TAG) --platform linux/amd64,linux/arm64 --output type=$(DOCKER_BUILDX_OUTPUT) --target temporal-admin-tools

##### Grafana #####
update-dashboards:
	@printf $(COLOR) "Update dashboards submodule from remote..."
	git submodule update --force --init --remote develop/docker-compose/grafana/provisioning/temporalio-dashboards

##### Auxiliary #####
gomodtidy:
	@printf $(COLOR) "go mod tidy..."
	@go mod tidy

update-dependencies:
	@printf $(COLOR) "Update dependencies..."
	@go get -u -t $(PINNED_DEPENDENCIES) ./...
	@go mod tidy

ensure-no-changes:
	@printf $(COLOR) "Check for local changes..."
	@printf $(COLOR) "========================================================================"
	@git diff --name-status --exit-code || (printf $(COLOR) "========================================================================"; printf $(RED) "Above files are not regenerated properly. Regenerate them and try again."; exit 1)
