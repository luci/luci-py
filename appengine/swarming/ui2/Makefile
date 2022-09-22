export SHELL = /bin/bash
export PATH := $(CURDIR)/nodejs/bin:$(PATH)
export npm_config_cache := $(CURDIR)/.npm

.ONESHELL:

default: _packages-lock.json.done
	npx webpack --mode=development

release: _packages-lock.json.done
	npx webpack --mode=production

serve: _packages-lock.json.done
	npx webpack-dev-server --mode=development

test: _packages-lock.json.done
	npx karma start

browser_test:
	# Good for debugging/developing tests.
	# Starts up the server so one can navigate to http://0.0.0.0:9876 and run the
	# tests there, seeing the results. Unfortunately, due to how karma needs to
	# concatenate the _test.js files and run them through webpack, it doesn't
	# autodetect changes and needs to be re-run manually :(
	npx karma start --no-single-run

continuous_test:
	# Good for debugging/developing tests.
	# Runs the tests every time a file changes in ./modules/
	# Works on linux only, with an apt-get install inotify-tools
	make test
	while inotifywait -e modify ./modules/*; do
		make test
	done

format: _packages-lock.json.done
	git ls-files "[^dist]*.js" | xargs npx eslint --fix --quiet

_packages-lock.json.done: package-lock.json
	npm ci
	touch _packages-lock.json.done

.PHONY: docs
docs: package-lock.json
	npx jsdoc -c jsdoc.config.js -d docs
	xdg-open docs/index.html

install_deps:
	npm install
