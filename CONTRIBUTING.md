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

4.  Open a pull request. CI runs the same test command on every supported Python
    version, plus one leg with no third-party dependencies installed.

### Reporting issues

Please use the
[GitHub issue tracker](https://github.com/google/lightflow/issues) for bug
reports and feature requests. Security issues should be reported through
[Google's Open Source Software Vulnerability Reward Program](https://bughunters.google.com/open-source-security)
rather than as public issues.
