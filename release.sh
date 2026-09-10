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

# The sed above changes nothing if the file has no `__version__` assignment
# to rewrite, which would otherwise release a version the package never
# declares.
if ! grep -q "__version__ = \"${VERSION}\"" "${version_file}"; then
  echo "ERROR: ${version_file} does not declare __version__ = \"${VERSION}\" after the version bump"
  echo "Check that the file contains a literal '__version__ = \"X.X.X\"' assignment."
  exit 1
fi

# Keep the library's CHANGELOG in step with the release, so the two can't drift
# apart. It handles three cases.
#   - When "## [Unreleased]" exists, promote it to this version, dated today,
#     and start a fresh empty [Unreleased] section above it.
#   - When there is no CHANGELOG at all, create one and warn.
#   - When the CHANGELOG has no [Unreleased] section, add a dated heading for
#     this version and warn.
# If the changelog already has an entry for this version, the file is left
# untouched, so running the same release twice changes nothing.
changelog_file="libraries/${PACKAGE}/CHANGELOG.md"
release_date=$(date +%Y-%m-%d)

if [ ! -f "$changelog_file" ]; then
  cat > "$changelog_file" <<EOF
# Changelog

All notable changes to this integration will be documented in this file.

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.0.0/).

## [Unreleased]

## [${VERSION}] - ${release_date}

- See the git history for the changes in this release.
EOF
  echo "WARNING: ${PACKAGE} had no CHANGELOG.md; created one with an entry for ${VERSION}"
elif grep -q "^## \[\{0,1\}${VERSION}\]\{0,1\}\( \|$\)" "$changelog_file"; then
  echo "CHANGELOG already documents ${VERSION}; leaving it unchanged"
elif grep -q '^## \[Unreleased\]' "$changelog_file"; then
  awk -v ver="$VERSION" -v date="$release_date" '
    !promoted && /^## \[Unreleased\]/ {
      print "## [Unreleased]"
      print ""
      print "## [" ver "] - " date
      promoted = 1
      next
    }
    { print }
  ' "$changelog_file" > "${changelog_file}.tmp" && mv "${changelog_file}.tmp" "$changelog_file"
  echo "Promoted [Unreleased] to [${VERSION}] in ${changelog_file}"
else
  # No [Unreleased] to promote. Insert a dated heading above the newest existing
  # version section, or at the end of the file if there are none.
  awk -v ver="$VERSION" -v date="$release_date" '
    !inserted && /^## / {
      print "## [" ver "] - " date
      print ""
      print "- See the git history for the changes in this release."
      print ""
      inserted = 1
    }
    { print }
    END {
      if (!inserted) {
        print ""
        print "## [" ver "] - " date
        print ""
        print "- See the git history for the changes in this release."
      }
    }
  ' "$changelog_file" > "${changelog_file}.tmp" && mv "${changelog_file}.tmp" "$changelog_file"
  echo "WARNING: ${changelog_file} has no [Unreleased] section; added a bare entry for ${VERSION}"
fi

git add "${version_file}" "${changelog_file}"
if git diff --staged --quiet; then
  echo "Version is already ${VERSION}; skipping release commit"
else
  git diff --staged
  git commit -m "Release $PACKAGE $VERSION"
  git push
fi

RELEASE_TAG="${PACKAGE//-/_}-${VERSION}"

echo "Release ${RELEASE_TAG}?"
read -r -p "Press enter to continue..."

git tag "$RELEASE_TAG"
git push origin "$RELEASE_TAG"
