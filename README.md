Modelcap is a [Nix](www.nixos.org/nix) like library for lightweight
state-tracking in Python.

Modelcap
--------

Features:

* Modelcap is a tool for tracking immutable application-defined states in Python
* Basic (and currently the only) base classes are:
  - _Config_, which represents JSON-serializable configuration. Configuration
    may contain references to other states in storage.
  - _Model_, for moving the state across its lifecycle from creation to
    sealing into the storage.
* For Models, there are methods to keep record of non-determenistic state
  modifications via JSON-serializable abstractions called _Program_s.
* A small collection of functions for search and tracking dependencies does
  exist.
* Mypy-based typing
* No extra ependencies
* <1K lines of code

The main usecase for this library is managing long multi-staged training of Machine
learning models which is required by modern NLP. The ML-specific parts will be
released soon as a separate library.
