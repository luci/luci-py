default: package-lock.json
	npx webpack --mode=development

release: package-lock.json
	npx webpack --mode=production
	cp node_modules/@webcomponents/custom-elements/custom-elements.min.js dist/custom-elements.min.js

serve: package-lock.json
	npx webpack-dev-server --mode=development --watch-poll

test: default
	# Run the generated tests just once under Xvfb.
	xvfb-run --auto-servernum --server-args "-screen 0 1280x1024x24" npx karma start

dev_test: default
	# Run the generated tests continiously under Xvfb.
	xvfb-run --auto-servernum --server-args "-screen 0 1280x1024x24" npx karma start --no-single-run

package-lock.json: package.json
	npm install
