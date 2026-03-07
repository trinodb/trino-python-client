#!/usr/bin/env python3
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Fetches the official Python release cycle and updates project files to reflect
the current set of supported Python versions.

Run from the project root:
    python .github/scripts/update_python_versions.py

Exit code 0 on success (whether or not files were changed).
The caller can inspect `git diff --quiet` to decide whether to open a PR.
"""
import json
import re
import sys
import urllib.request
from typing import Any
from typing import Callable

RELEASE_CYCLE_URL = (
    "https://raw.githubusercontent.com/python/devguide/main/include/release-cycle.json"
)


def _require_sub(
    pattern: str, repl: str | Callable[[re.Match[str]], str], string: str, label: str, **kwargs: Any
) -> str:
    """Like re.sub(), but warns on stderr when the pattern does not match."""
    result, count = re.subn(pattern, repl, string, **kwargs)
    if count == 0:
        print(f"WARNING: no match for {label} pattern: {pattern}", file=sys.stderr)
    return result


def fetch_release_cycle() -> dict[str, Any]:
    with urllib.request.urlopen(RELEASE_CYCLE_URL, timeout=30) as response:
        return json.loads(response.read())


def version_key(version: str) -> tuple[int, ...]:
    return tuple(int(x) for x in version.split("."))


def compute_version_sets(cycle: dict[str, Any]) -> tuple[set[str], set[str]]:
    """Return (eol, active) sets of '3.X' version strings."""
    eol = set()
    active = set()
    for version, info in cycle.items():
        if not re.match(r"^3\.\d+$", version):
            continue
        status = info.get("status", "")
        if status == "end-of-life":
            eol.add(version)
        elif status in ("bugfix", "security"):
            active.add(version)
    return eol, active


def get_current_versions(setup_content: str) -> set[str]:
    """Extract '3.X' versions from setup.py Programming Language classifiers."""
    return set(re.findall(r'"Programming Language :: Python :: (3\.\d+)"', setup_content))


def update_setup_py(content: str, to_add: set[str], to_remove: set[str], min_active: str) -> str:
    # Remove EOL classifiers (exact line match: 8 spaces + string + comma + newline)
    for version in sorted(to_remove):
        content = re.sub(
            r'        "Programming Language :: Python :: ' + re.escape(version) + r'",\n',
            "",
            content,
        )

    # Insert new classifiers before the Implementation classifiers
    if to_add:
        new_lines = "\n".join(
            '        "Programming Language :: Python :: ' + version + '",'
            for version in sorted(to_add, key=version_key)
        ) + "\n"
        content = _require_sub(
            r'(        "Programming Language :: Python :: Implementation :: CPython")',
            new_lines + r"\1",
            content,
            "setup.py CPython classifier anchor",
            count=1,
        )

    # Update python_requires minimum
    content = _require_sub(
        r'python_requires=">=3\.\d+"',
        f'python_requires=">={min_active}"',
        content,
        "setup.py python_requires",
    )
    return content


def update_tox_ini(content: str, active_versions: set[str]) -> str:
    envlist = ",".join(
        "py" + v.replace(".", "")
        for v in sorted(active_versions, key=version_key)
    )
    return _require_sub(r"^envlist = .*$", f"envlist = {envlist}", content, "tox.ini envlist", flags=re.MULTILINE)


def update_ci_yml(content: str, active_versions: set[str], eol_versions: set[str], latest_stable: str) -> str:
    sorted_versions = sorted(active_versions, key=version_key)

    # Replace the entire `python: [...]` matrix block, preserving non-EOL PyPy entries
    def rebuild_python_list(m: re.Match[str]) -> str:
        existing_pypy = re.findall(r'"pypy-(3\.\d+)"', m.group(2))
        # Keep order, deduplicate, drop EOL
        seen: set[str] = set()
        valid_pypy: list[str] = []
        for v in existing_pypy:
            if v not in eol_versions and v not in seen:
                seen.add(v)
                valid_pypy.append(v)
        lines = [f'          "{v}",' for v in sorted_versions]
        lines += [f'          "pypy-{v}",' for v in valid_pypy]
        return m.group(1) + "\n" + "\n".join(lines) + m.group(3)

    content = _require_sub(
        r"(        python: \[)(.*?)(\n        \])",
        rebuild_python_list,
        content,
        "ci.yml python matrix",
        flags=re.DOTALL,
    )

    # Update the checks job python-version pin
    content = _require_sub(
        r'(python-version: )"3\.\d+"',
        f'\\1"{latest_stable}"',
        content,
        "ci.yml python-version pin",
    )

    # Update include entries to use latest_stable.
    # Include entries use `python: "X.Y"` format (distinct from the matrix list).
    # Find whatever version they currently pin (the highest one = last "latest") and bump it.
    current_include_version = re.findall(r'python: "(\d+\.\d+)"', content)
    if current_include_version:
        old_include_version = max(current_include_version, key=version_key)
        if old_include_version != latest_stable:
            content = content.replace(
                f'python: "{old_include_version}"',
                f'python: "{latest_stable}"',
            )

    return content


def update_release_yml(content: str, latest_stable: str) -> str:
    return _require_sub(
        r'(PYTHON_VERSION: )"3\.\d+"',
        f'\\1"{latest_stable}"',
        content,
        "release.yml PYTHON_VERSION",
    )


def update_readme(content: str, min_active: str) -> str:
    return _require_sub(r"Python>=3\.\d+", f"Python>={min_active}", content, "README.md Python>= mention")


def update_pre_commit(content: str, min_active: str) -> str:
    compact = min_active.replace(".", "")
    return _require_sub(r"--py\d+-plus", f"--py{compact}-plus", content, ".pre-commit-config.yaml --pyXX-plus")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    print("Fetching Python release cycle data...")
    try:
        cycle = fetch_release_cycle()
    except Exception as exc:
        print(f"ERROR fetching release cycle: {exc}", file=sys.stderr)
        sys.exit(1)

    eol, active = compute_version_sets(cycle)
    if not active:
        print("ERROR: No active Python versions found in release cycle data.", file=sys.stderr)
        sys.exit(1)

    latest_stable = max(active, key=version_key)
    min_active = min(active, key=version_key)

    print(f"Active versions:  {sorted(active, key=version_key)}")
    print(f"EOL versions:     {sorted(eol, key=version_key)}")
    print(f"Latest stable:    {latest_stable}")
    print(f"Minimum active:   {min_active}")

    with open("setup.py", encoding="utf-8") as f:
        setup_content = f.read()
    current = get_current_versions(setup_content)
    to_add = active - current
    to_remove = current & eol

    print(f"Currently in setup.py: {sorted(current, key=version_key)}")
    print(f"To add:    {sorted(to_add, key=version_key)}")
    print(f"To remove: {sorted(to_remove, key=version_key)}")

    files: dict[str, Callable[[str], str]] = {
        "setup.py": lambda c: update_setup_py(c, to_add, to_remove, min_active),
        "tox.ini": lambda c: update_tox_ini(c, active),
        ".github/workflows/ci.yml": lambda c: update_ci_yml(c, active, eol, latest_stable),
        ".github/workflows/release.yml": lambda c: update_release_yml(c, latest_stable),
        "README.md": lambda c: update_readme(c, min_active),
        ".pre-commit-config.yaml": lambda c: update_pre_commit(c, min_active),
    }

    changed = False
    for path, updater in files.items():
        with open(path, encoding="utf-8") as f:
            original = f.read()
        updated = updater(original)
        if updated != original:
            with open(path, "w", encoding="utf-8") as f:
                f.write(updated)
            print(f"Updated: {path}")
            changed = True

    if not changed:
        print("No changes needed.")
    else:
        print("All files updated successfully.")


if __name__ == "__main__":
    main()
