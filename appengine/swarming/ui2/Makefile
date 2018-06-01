default: package-lock.json
	npx webpack --mode=development

release: package-lock.json
	npx webpack --mode=production
	cp node_modules/@webcomponents/custom-elements/custom-elements.min.js dist/custom-elements.min.js

serve: package-lock.json
	npx webpack-dev-server --mode=development --watch-poll

package-lock.json: package.json
	npm install
