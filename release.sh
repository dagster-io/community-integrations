#!/bin/bash
#
# Releases <package> at specified <version>.
#
# USAGE
#
#     ./release.sh dagster-anthropic 0.0.2
#

if [ "$#" -ne 2 ]; then
    echo "Usage: $0 <library> <version>"
    exit 1
fi

PACKAGE="$1"
VERSION="$2"

if [ ! -d "libraries/${PACKAGE}" ]; then
  echo "ERROR: Package libraries/${PACKAGE} does not exist"
  exit 1
fi

if [[ ! $VERSION =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
  echo "ERROR: ${VERSION} does not match pattern X.X.X"
  exit 1
fi

# Find the file containing the `__version__ = "X.X.X"` definition.
version_file=$(grep -lr '__version__ = "' libraries/${PACKAGE})

sed -i '' 's/__version__ = "[^"]*"/__version__ = "'"$VERSION"'"/' "${version_file}"

git add "${version_file}"
git commit -m "Release $PACKAGE $VERSION"

RELEASE_TAG="${PACKAGE//-/_}-${VERSION}"

echo "Release ${RELEASE_TAG}?"
read -r -p "Press enter to continue..."

git tag "$RELEASE_TAG"
git push origin "$RELEASE_TAG"
