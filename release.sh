#!/bin/bash
set -euo pipefail

# Valid steps are: major, minor, patch, premajor, preminor, prepatch, prerelease
# See https://python-poetry.org/docs/cli/#version
select step in major minor patch prerelease; do
  break
done

if [ -z "$step" ]; then
  echo "No valid step selected. Exiting."
  exit 1
fi

old_version=$(poetry version -s)

poetry version "$step"

new_version=$(poetry version -s)

git commit -am "Bump version from $old_version to $new_version"

git push

git tag --force "v$new_version"

git push --force origin "v$new_version"
