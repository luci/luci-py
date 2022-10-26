# Copyright 2015 The LUCI Authors. All rights reserved.
# Use of this source code is governed under the Apache License, Version 2.0
# that can be found in the LICENSE file.
#

TARDIR ?= "/workspace"
define FILEBUG

\e[31m
If you're using gae.py due to deploy/deploy_dev targets not working
please file a bug at:

https://bugs.chromium.org/p/chromium/issues/entry?components=Infra%3EProdTech%3EReleases

If not, please use the deploy/deploy_dev targets.
\e[0m

endef
export FILEBUG

build: compile-proto

compile-proto:
	cd components/config/proto && make

# deploy_dev-gae deploys to the dev environmant using gae.py.
# When using this due to deploy/deploy_dev targets not working, please file a bug at:
#
# https://bugs.chromium.org/p/chromium/issues/entry?components=Infra%3EProdTech%3EReleases
deploy_dev_gae: build
	@echo -e "$$FILEBUG"
	tools/gae upload -x -A luci-config-dev

# deploy_dev-gae deploys to prod using gae.py.
# When using this due to deploy/deploy-dev targets not working, please file a bug at:
#
# https://bugs.chromium.org/p/chromium/issues/entry?components=Infra%3EProdTech%3EReleases
deploy_gae: build
	@echo -e "$$FILEBUG"
	tools/gae upload -x -A luci-config

# deploy sends the currently submitted version off to Spinnaker for deployment.
# Spinnaker will first deploy to staging and then prod.
#
# See go/luci-config-spin for Spinnaker pipeline status.
deploy:
	gcloud builds submit --no-source --substitutions=_PROJECT_DIR=luci/appengine/config_service --config cloudbuild.yaml --project chrome-infra-spinnaker
	echo "Follow deployment at: go/luci-config-spin"

# deploy_dev packs up the current folder and sends it off to Spinnaker for release to the Dev environment.
#
# See go/luci-config-spin for Spinnaker pipeline status.
deploy_dev: build
	$(eval GCBDIR := $(shell mktemp -d -p /tmp luci-config.XXXX))
	# The protoc symlink points to nowhere.
	# Is that expected?
	rsync -aLK --exclude="tools/protoc" . $(GCBDIR)
	cd $(GCBDIR) && gcloud builds submit --config cloudbuild_dev.yaml . --project chrome-infra-spinnaker
	rm -fr $(GCBDIR)
	@echo "Follow deployment at: go/luci-config-spin"

# external_deps fetches and builds external depencencies.
# This is run but GCB in the build stage.
external_deps:
	cd ui && make

# package_release packages the current folder to TARDIR.
package_release:
	rsync -aLK . $(TARDIR)/package
