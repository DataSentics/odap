# ODAP Framework

## Overview

ODAP is a lightweight framework for writing features, creating and exporting
segments to various destinations (e.g. Facebook, Salesforce, etc.)

Both SQL and Pyspark syntax is supported.

It's build on top of the Databricks platform.

You can try the framework right now by cloning [demo project](https://github.com/DataSentics/features-factory-demo) to your Databricks Workspace.

## Documentation
For documentation see [ODAP Documentation](https://github.com/DataSentics/odap).

## Development
There are two main components (sub-packages)
- `feature_factory` - responsible for features development and orchestration
- `segment_factory` - responsible for segments creation and exports

### Dependency management
Use `poetry` as main dependency management tool

### Linting & Formatting
- pylint
- pyre-check
- black

### Code style
- functions-only python (no dependency injection)
- try to avoid classes as much as possible
- data classes are OK
- no `__init__.py` files
- keep the `src` directory in root
- project config is raw YAML
- use type hinting as much as possible
