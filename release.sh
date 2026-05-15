#!/bin/bash
#
# Releases <package> at specified <version>.
#
# USAGE
#
#     ./release.sh dagster-anthropic 0.0.2
#     ./release.sh dagster-anthropic          # Auto-bumps patch version
#

if [ "$#" -lt 1 ] || [ "$#" -gt 2 ]; then
    echo "Usage: $0 <library> [version]"
    echo "  If version is not provided, the patch version will be auto-incremented"
    exit 1
fi

PACKAGE="$1"

if [ ! -d "libraries/${PACKAGE}" ]; then
  echo "ERROR: Package libraries/${PACKAGE} does not exist"
  exit 1
fi

# Find the file containing the `__version__ = "X.X.X"` definition.
# Convert package name from kebab-case to snake_case (e.g., dagster-anthropic -> dagster_anthropic)
package_underscored="${PACKAGE//-/_}"

# Try different possible locations for the version file
# Prefer version.py over __init__.py since some packages import __version__ from version.py
if [ -f "libraries/${PACKAGE}/src/${package_underscored}/version.py" ]; then
  version_file="libraries/${PACKAGE}/src/${package_underscored}/version.py"
elif [ -f "libraries/${PACKAGE}/${package_underscored}/version.py" ]; then
  version_file="libraries/${PACKAGE}/${package_underscored}/version.py"
elif [ -f "libraries/${PACKAGE}/src/${package_underscored}/__init__.py" ]; then
  version_file="libraries/${PACKAGE}/src/${package_underscored}/__init__.py"
elif [ -f "libraries/${PACKAGE}/${package_underscored}/__init__.py" ]; then
  version_file="libraries/${PACKAGE}/${package_underscored}/__init__.py"
else
  echo "ERROR: Could not find version file for ${PACKAGE}"
  echo "Tried:"
  echo "  - libraries/${PACKAGE}/src/${package_underscored}/version.py"
  echo "  - libraries/${PACKAGE}/${package_underscored}/version.py"
  echo "  - libraries/${PACKAGE}/src/${package_underscored}/__init__.py"
  echo "  - libraries/${PACKAGE}/${package_underscored}/__init__.py"
  exit 1
fi

# If version is provided, use it. Otherwise, auto-bump the patch version.
if [ "$#" -eq 2 ]; then
  VERSION="$2"

  if [[ ! $VERSION =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
    echo "ERROR: ${VERSION} does not match pattern X.X.X"
    exit 1
  fi
else
  # Extract current version from the version file
  current_version=$(grep -o '__version__ = "[^"]*"' "${version_file}" | cut -d'"' -f2)

  if [ -z "$current_version" ]; then
    echo "ERROR: Could not extract current version from ${version_file}"
    exit 1
  fi

  # Parse version components
  IFS='.' read -r major minor patch <<< "$current_version"

  # Increment patch version
  patch=$((patch + 1))
  VERSION="${major}.${minor}.${patch}"

  echo "Auto-bumping version from ${current_version} to ${VERSION}"
fi

sed -i '' 's/__version__ = "[^"]*"/__version__ = "'"$VERSION"'"/' "${version_file}"

git add "${version_file}"
git diff --staged
git commit -m "Release $PACKAGE $VERSION"
git push

RELEASE_TAG="${PACKAGE//-/_}-${VERSION}"

echo "Release ${RELEASE_TAG}?"
read -r -p "Press enter to continue..."

git tag "$RELEASE_TAG"
git push origin "$RELEASE_TAG"
