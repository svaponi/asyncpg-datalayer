#!/bin/bash
set -euo pipefail

# Valid steps are: major, minor, patch, premajor, preminor, prepatch, prerelease
# See https://python-poetry.org/docs/cli/#version
step=${1:-patch}

old_version=$(poetry version -s)

poetry version "$step"

new_version=$(poetry version -s)

git commit -am "Bump version from $old_version to $new_version"

git tag -a "v$new_version" -m "Release v$new_version"

git push --follow-tags
