<!-- mdformat global-off -->

# How to Contribute

We would love to accept your patches and contributions to this project.

## Before you begin

### Sign our Contributor License Agreement

Contributions to this project must be accompanied by a
[Contributor License Agreement](https://cla.developers.google.com/about) (CLA).
You (or your employer) retain the copyright to your contribution; this simply
gives us permission to use and redistribute your contributions as part of the
project.

If you or your current employer have already signed the Google CLA (even if it
was for a different project), you probably don't need to do it again.

Visit <https://cla.developers.google.com/> to see your current agreements or to
sign a new one.

### Review our Community Guidelines

This project follows
[Google's Open Source Community Guidelines](https://opensource.google/conduct/)
and the [Code of Conduct](./CODE_OF_CONDUCT.md) in this repository.

## Architecture & Codebase Layout

| Module | Responsibility |
| :--- | :--- |
| `lightflow/schema.py` & `lightflow/parsers.py` | Pure-Python dataclass models (`Lightflow`, `Stage`, `Passport`, `Stamp`) and YAML/JSON/textproto loaders, including zero-dependency stdlib fallbacks when `PyYAML` or `jsonschema` are not installed. The dataclasses expose Protobuf-style compatibility methods (`HasField`, `CopyFrom`, `ClearField`) so the core engine and state modules remain portable across both pure-Python and Protocol Buffer environments. |
| `lightflow/cel_lite.py` | Safe AST-based CEL-like expression parser and evaluator with memory/repetition bounds. |
| `lightflow/compiler.py` | DAG topological sorting, action/stage validation, and compile-time `payload.outputs.<stage>` dependency checks (`CompilerMixin`). |
| `lightflow/executor.py` | Action import resolution, `LIGHTFLOW_ALLOWED_IMPORT_PREFIXES` sandboxing, `dry_run` simulation, timeout worker threads, and `rollback_action` execution (`ActionExecutorMixin`). |
| `lightflow/payload.py` | Payload merging, per-stage output attribution (`payload.outputs.<stage>`), `secret_keys` redaction, and `passport.json` serialization. |
| `lightflow/engine.py` | `LightflowEngine`: trigger-rule evaluation and single-stage execution (`python_action`, `operator_action`, `polling_policy`). |
| `lightflow/state.py` & `lightflow/dry_run.py` | `PassportManager` (`flock`/`msvcrt` advisory locking, atomic writes), run state classification, subgraph re-arming helpers, and `dry_run` / Mermaid rendering. |
| `lightflow/lib.py` & `lightflow/runner.py` | `LightflowRunnerCLI` (`start`, `resume`, `status`, `list`, `dry_run`, `visualize`, `cleanup`) and CLI argument dispatching. |
| `lightflow/visualizer.py` & `visualizer.html` | Standalone zero-CDN HTML5 DAG and execution trace visualizer. |

## Contribution process

### Code Reviews

All submissions, including submissions by project members, require review. We
use [GitHub pull requests](https://docs.github.com/articles/about-pull-requests)
for this purpose.

1.  Fork the repository and create a topic branch from `main`.
2.  Make your change. Keep the standard-library fallbacks working: the test
    suite is expected to pass both with and without `PyYAML` / `jsonschema`
    installed.
3.  Add or update tests under `tests/` and run them locally:

    ```bash
    pip install -e .
    python -m unittest discover -s tests -v
    ```

4.  Open a pull request. GitHub Actions CI will run the test matrix across
    Linux, macOS, and Windows (including the zero-dependency standard-library
    fallback job).

### Reporting issues

Please use the
[GitHub issue tracker](https://github.com/google/lightflow/issues) for bug
reports and feature requests. Security issues should be reported through
[Google's Open Source Software Vulnerability Reward Program](https://bughunters.google.com/open-source-security)
rather than as public issues.
