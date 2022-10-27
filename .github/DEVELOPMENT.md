# Development

Developers should read [the development section of the website](https://trino.io/development),
which covers things like development philosophy and contribution process.

* [Getting started](#getting-started)
* [Running tests](#running-tests)
* [Commits and pull requests](#commits-and-pull-requests)
* [Code style](#code-style)
* [Releasing](#releasing)

## Getting started

Start by forking the repository and then modify the code in your fork.

We recommend that you use Python3's `venv` for development:

```
python3 -m venv .venv
. .venv/bin/activate
pip install -e '.[tests]'
```

With `-e` passed to `pip install` above pip can reference the code you are
modifying in the *virtual env*. That way, you do not need to run `pip install`
again to make your changes applied to the *virtual env*.

## Running tests

`trino-python-client` uses [pytest](https://pytest.org/) for its tests. To run
only unit tests, type:

```
pytest tests/unit
```

Then you can pass options like `--pdb` or anything supported by `pytest --help`.

To run integration tests:

```
pytest tests/integration
```

They pull a Docker image and then run a container with a Trino server:
- the image is named `trinodb/trino:${TRINO_VERSION}`
- the container is named `trino-python-client-tests-{uuid4()[:7]}`

To run the tests with different versions of Python in managed *virtual envs*,
use `tox` (see the configuration in `tox.ini`):

```
tox
```

When the code is ready, submit a Pull Request.

## Commits and pull requests

See [Commits and pull requests](https://github.com/trinodb/trino/blob/master/.github/DEVELOPMENT.md#commits-and-pull-requests) section from Trino.

## Code style

To run linting and formatting checks before opening a PR: `pip install pre-commit && pre-commit run --all-files`

Code can also be automatically checked on commit by a [pre-commit](https://pre-commit.com/) git hook by executing `pre-commit install`.

In addition to that you should also adhere to the following:

### Readability

Prefer code that is readable over one that is "clever". The purpose of code
style rules is to maintain code readability and developer efficiency when
working with the code. All the code style rules explained below are good
guidelines to follow but there may be exceptional situations where we
purposefully depart from them. When readability and code style rule are at
odds, the readability is more important.

### Consistency

Keep code consistent with surrounding code where possible.

### Avoid mocks where possible

Do not use mocking libraries. These libraries encourage testing specific call
sequences, interactions, and other internal behavior, which we believe leads to
fragile tests. They also make it possible to mock complex interfaces or
classes, which hides the fact that these classes are not (easily) testable. We
prefer to write mocks by hand, which forces code to be written in a certain
testable style.

We also acknowledge that there is existing code which uses mocks but that
should not be taken as a reason increase reliance on mocks.

### Maintain production quality for test code

Maintain the same quality for production and test code.

### Avoid abbreviations

Please avoid abbreviations, slang or inside jokes as this makes harder for
non-native english speaker to understand the code. Very well known
abbreviations like `max` or `min` and ones already very commonly used across
the code base like `ttl` are allowed and encouraged.

## Releasing

- [Set up your development environment](#getting-started).
- Check the local workspace is up to date and has no uncommitted changes
  ```bash
  git fetch -a && git status
  ```
- Change version in `trino/__init__.py` to a new version, e.g. `0.123.0`.
- Commit
  ```bash
  git commit -a -m "Bump version to 0.123.0"
  ```
- Create an annotated tag
  ```bash
  git tag -m "" 0.123.0
  ```
- Create release package and upload it to PyPI
  ```bash
  . .venv/bin/activate && \
  pip install twine wheel setuptools && \
  rm -rf dist/ && \
  ./setup.py sdist bdist_wheel && \
  twine upload dist/* && \
  open https://pypi.org/project/trino/ && \
  echo "Released!"
  ```
- Push the branch and the tag
  ```bash
  git push upstream master 0.123.0
  ```
- Send release announcement on the *#python-client* channel on [Trino Slack](https://trino.io/slack.html).
