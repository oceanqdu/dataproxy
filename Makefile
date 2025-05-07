# Setting SHELL to bash allows bash commands to be executed by recipes.
# Options are set to exit when a recipe line exits non-zero or a piped command fails.
SHELL = /usr/bin/env bash -o pipefail
.SHELLFLAGS = -ec

.PHONY: all
all: build

##@ General

# The help target prints out all targets with their descriptions organized
# beneath their categories. The categories are represented by '##@' and the
# target descriptions by '##'. The awk commands is responsible for reading the
# entire set of makefiles included in this invocation, looking for lines of the
# file as xyz: ## something, and then pretty-format the target and help. Then,
# if there's a line with ##@ something, that gets pretty-printed as a category.
# More info on the usage of ANSI control characters for terminal formatting:
# https://en.wikipedia.org/wiki/ANSI_escape_code#SGR_parameters
# More info on the awk command:
# http://linuxcommand.org/lc3_adv_awk.php

.PHONY: help
help: ## Display this help.
	@awk 'BEGIN {FS = ":.*##"; printf "\nUsage:\n  make \033[36m<target>\033[0m\n"} /^[a-zA-Z_0-9-]+:.*?##/ { printf "  \033[36m%-16s\033[0m %s\n", $$1, $$2 } /^##@/ { printf "\n\033[1m%s\033[0m\n", substr($$0, 5) } ' $(MAKEFILE_LIST)


##@ Development

.PHONY: test
test: ## Run tests.
	mvn clean test

.PHONY: build
build: ## Build DataProxy binary whether to integrate frontend.
	./scripts/build.sh

.PHONY: image
image: build ## Build docker image with the manager.
	./scripts/build_image.sh

.PHONY: docs
docs: ## Build docs.
	cd docs && pip install -r requirements.txt && make html

.PHONY: test-integration
test-integration: ## Run integration tests.
	mvn clean test -DenableIntegration=true

BAZEL_REPO_CACHE ?= /tmp/bazel_repo_cache
BAZEL_MAX_JOBS ?= 16
.PHONY: sdk_cache_update
sdk_cache_update: ## Update sdk cache.
	cd dataproxy_sdk/cc && \
	bazel fetch :dataproxy_sdk_cc --repository_cache=$(BAZEL_REPO_CACHE)
	cd dataproxy_sdk/python && \
	bazel fetch :dataproxy_sdk_py --repository_cache=$(BAZEL_REPO_CACHE)

.PHONY: sdk_cc_test
sdk_cc_test: ## Run sdk c++ tests.
	cd dataproxy_sdk/cc && \
	bazel coverage //test:all -c opt --combined_report=lcov --output_filter=^//: \
		--jobs $(BAZEL_MAX_JOBS) --instrumentation_filter=^// \
		--repository_cache=$(BAZEL_REPO_CACHE) --nocache_test_results

.PHONY: sdk_py_test
sdk_py_test: ## Run sdk python tests.
	cd dataproxy_sdk/python && \
	bazel coverage //test:all -c opt --combined_report=lcov --output_filter=^//: \
		--jobs $(BAZEL_MAX_JOBS) --instrumentation_filter=^// \
		--repository_cache=$(BAZEL_REPO_CACHE) --nocache_test_results

.PHONY: sdk_go_test
sdk_go_test: ## Run sdk go tests.
	cd dataproxy_sdk/go && \
	rm -rf ./test-results && mkdir -p test-results && \
	GOEXPERIMENT=nocoverageredesign go test \
		$$(go list ./pkg/... | grep -Ev "crd|testing|test") \
		--parallel 4 -gcflags="all=-N -l" \
		-coverprofile=test-results/pkg.covprofile.out | tee test-results/pkg.output.txt
	cd dataproxy_sdk/go && cat test-results/pkg.output.txt | go-junit-report > test-results/TEST-pkg.xml
	echo "mode: set" > dataproxy_sdk/go/test-results/coverage.out && \
		cat dataproxy_sdk/go/test-results/*.covprofile.out | grep -v mode: | sort -r | awk '{if($$1 != last) {print $0;last=$$1}}' >> dataproxy_sdk/go/test-results/coverage.out
	cd dataproxy_sdk/go && cat test-results/coverage.out | gocover-cobertura > test-results/coverage.xml

.PHONY: sdk_test
sdk_test: sdk_cc_test sdk_py_test sdk_go_test

PYTHON_VERSION ?= $(shell python3 -c "import sys; print(f'{sys.version_info.major}.{sys.version_info.minor}')")
.PHONY: sdk_py_build
sdk_py_build: ## Build sdk python package.
	cd dataproxy_sdk/python && \
	bazel build //:dataproxy_sdk_whl -c opt \
		--@rules_python//python/config_settings:python_version=$(PYTHON_VERSION) \
		--repository_cache=$(BAZEL_REPO_CACHE)
