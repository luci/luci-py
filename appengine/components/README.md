components
----------

Modules and tools in this directory are shared between all AppEngine servers in
this repository under /appengine.

Contents of this directory:

  * components/ - contains reusable code to be used from AppEngine application.
    'components' directory must be symlinked to an app directory (under same
    name 'components'). Must contain only code that needs to be deployed to GAE
    The only exception is unit tests with regexp '.+_test\.py' or 'test_.+',
    which are acceptable.
  * components/third_party/ - third party code that gets deployed to GAE
    together with components. Contains third party dependencies of components
    code.
  * test_support/ - reusable code that can only be used from tests, not from
    GAE. This code must not be deployed to GAE.
  * tool_support/ - reusable code that can only be used from tests and tools,
    not from GAE. This code must not be deployed to GAE.
  * support/ - reusable code that can only be used from tests and tools, not
    from GAE. This code must not be deployed to GAE.
    TODO(maruel): Migrate each module to either test_support/ or tool_support/
  * tests/ - tests for components/, depends on third party code from
    third_party/ and also on support/. This code must not be deployed to GAE.
    TODO(maruel): Move every test into their respective directories.
  * tools/ - utilities to manage applications on GAE, depends on third_party/
    and support/. This code must not be deployed to GAE.
  * third_party/ - third party dependencies of tests and tools. This code must
    not be deployed to GAE. See components/third_party/ for third party
    dependencies that are deployed.
