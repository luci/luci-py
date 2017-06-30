# \<Config UI\>

This is a UI for the configuration service

## Install the Polymer-CLI

First, make sure you have the [Polymer CLI](https://www.npmjs.com/package/polymer-cli) installed. Then run `polymer serve` to serve your application locally.

## Viewing Your Application

```
$ polymer serve
```

## Building Your Application

```
$ polymer build
```

This will create builds of your application in the `build/` directory, optimized to be served in production. You can then serve the built versions by giving `polymer serve` a folder to serve from:

```
$ polymer serve build/default
```

## Running Tests

```
$ polymer test
```

Your application is already set up to be tested via [web-component-tester](https://github.com/Polymer/web-component-tester). Run `polymer test` to run your application's test suite locally.

## Third Party Files

In order to use proper authentication, the google-signin-aware element was needed. However, this element has not been updated to Polymer 2.0, so edits were made to the current version to ensure compatibility.
The modified google-signin-aware element can be found in the ui/common/third_party/google-signin folder.