.PHONY: clone_metabase_if_missing clean link_to_driver front_end driver update_deps_files server test all release
.DEFAULT_GOAL := all
export MB_EDITION=ee
export NODE_OPTIONS='--max-old-space-size=4096'

pinot_version := $(shell jq '.pinot' app_versions.json)
metabase_version := $(shell jq '.metabase' app_versions.json)

makefile_dir := $(dir $(realpath $(lastword $(MAKEFILE_LIST))))
pinot_port := 9000
is_pinot_started := $(shell curl --fail --silent --insecure http://localhost:$(pinot_port)/health | jq '.')

clone_metabase_if_missing:
ifeq ($(wildcard $(makefile_dir)metabase/.),)
	@echo "Did not find metabase repo, cloning version $(metabase_version)..."; git clone -b $(metabase_version) --depth 1 https://github.com/metabase/metabase.git
else
	@echo "Found metabase repo, skipping initialization."
endif

checkout_latest_metabase_tag: clone_metabase_if_missing clean
	cd $(makefile_dir)metabase; git fetch --all --tags;
	$(eval latest_metabase_version=$(shell cd $(makefile_dir)metabase; git tag | egrep 'v[0-9]+\.[0-9]+\.[0-9]+' | sort | tail -n 1))
	@echo "Checking out latest metabase tag: $(latest_metabase_version)"
	cd $(makefile_dir)metabase/modules/drivers && git checkout $(latest_metabase_version);
	sed -i.bak 's/metabase\": \".*\"/metabase\": \"$(latest_metabase_version)\"/g' app_versions.json; rm  ./app_versions.json.bak

start_pinot_if_missing:
ifeq ($(is_pinot_started),)
	@echo "Pinot not started, starting using version $(pinot_version)...";
	@echo "Please start your Pinot instance manually on port $(pinot_port)"
else
	@echo "Pinot started, skipping initialization."
endif

clean:
	@echo "Force cleaning Metabase repo..."
	cd $(makefile_dir)metabase/modules/drivers && git reset --hard && git clean -f
	@echo "Checking out metabase at: $(metabase_version)"
	cd $(makefile_dir)metabase/modules/drivers && git fetch --all --tags && git checkout $(metabase_version);

link_to_driver:
ifeq ($(wildcard $(makefile_dir)metabase/modules/drivers/pinot/src),)
	@echo "Adding link to driver..."; ln -s ../../../drivers/pinot $(makefile_dir)metabase/modules/drivers
else
	@echo "Driver found, skipping linking."
endif
	

front_end:
	@echo "Building Front End..."
	cd $(makefile_dir)metabase && yarn build-static-viz && \
	export WEBPACK_BUNDLE=production && yarn build-release && yarn build-static-viz

driver: update_deps_files
	@echo "Building Pinot driver..."
	cd $(makefile_dir)metabase/; ./bin/build-driver.sh pinot

server: 
	@echo "Starting metabase..."
	cd $(makefile_dir)metabase/; clojure -M:run

# This command adds the require pinot driver dependencies to the metabase repo.
update_deps_files:
	@if cd $(makefile_dir)metabase && grep -q pinot deps.edn; \
		then \
			echo "Metabase deps file updated, skipping..."; \
		else \
			echo "Updating metabase deps file..."; \
			cd $(makefile_dir)metabase/; sed -i.bak 's/\/test\"\]\}/\/test\" \"modules\/drivers\/pinot\/test\"\]\}/g' deps.edn; \
	fi

	@if cd $(makefile_dir)metabase/modules/drivers && grep -q pinot deps.edn; \
		then \
			echo "Metabase driver deps file updated, skipping..."; \
		else \
			echo "Updating metabase driver deps file..."; \
			cd $(makefile_dir)metabase/modules/drivers/; sed -i.bak "s/\}\}\}/\} \metabase\/pinot \{:local\/root \"pinot\"\}\}\}/g" deps.edn; \
	fi

test: start_pinot_if_missing link_to_driver update_deps_files
	@echo "Testing Pinot driver..."
	cd $(makefile_dir)metabase/; DRIVERS=pinot MB_PINOT_TEST_PORT=$(pinot_port) clojure -X:dev:drivers:drivers-dev:test

build: clone_metabase_if_missing update_deps_files link_to_driver front_end driver

docker-image:
	cd $(makefile_dir)metabase/; export MB_EDITION=ee && ./bin/build.sh && mv target/uberjar/metabase.jar bin/docker/ && docker build -t metabase-dev --build-arg MB_EDITION=ee ./bin/docker/ 