# Development

## Getting started with development

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

When the code is ready, submit a Pull Request.

### Code style

- For Python code, adhere to PEP 8.
- Prefer code that is readable over one that is "clever".
- When writing a Git commit message, follow these [guidelines](https://chris.beams.io/posts/git-commit/).

See also Trino's [guidelines](https://github.com/trinodb/trino/blob/master/.github/DEVELOPMENT.md).
Most of them also apply to code in trino-python-client.

### Running tests

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

## Releasing

- [Set up your development environment](#Getting-Started-With-Development).
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
