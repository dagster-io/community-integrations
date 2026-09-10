#!/bin/bash
#
# Generates CHANGELOG sections for a library from its git release tags.
#
# For each release of <library>, prints a dated version heading followed by one
# bullet per commit that touched that library between the previous release and
# this one. Release chores (version bumps, "Release <pkg> X.X.X") are filtered
# out, and `[dagster-foo] ` subject prefixes are stripped since the library is
# implied by the file the entry lives in.
#
# Release tags live on GitHub, so fetch them first.
#
#     git fetch origin --tags
#
# USAGE
#
#     ./scripts/generate_changelog.sh dagster-polars            # print to stdout
#     ./scripts/generate_changelog.sh dagster-polars > out.md   # save for review
#
# The output is a starting point for a maintainer to review and reword, not a
# finished changelog.

set -euo pipefail

if [ "$#" -ne 1 ]; then
  echo "Usage: $0 <library>" >&2
  echo "Example: $0 dagster-polars" >&2
  exit 1
fi

PACKAGE="$1"
PACKAGE_DIR="libraries/${PACKAGE}"

if [ ! -d "$PACKAGE_DIR" ]; then
  echo "ERROR: ${PACKAGE_DIR} does not exist" >&2
  exit 1
fi

# Six libraries were renamed out of `dagster-contrib-*` partway through their
# lives. Their early releases are tagged under the old prefix and their early
# commits touched the old directory, so both have to be searched or those
# releases come back empty. The map is deliberately hardcoded rather than
# derived from `git log --diff-filter=R`. dagster-qdrant is also a rename of
# `_template`, and following that would pull unrelated template history in.
case "$PACKAGE" in
  dagster-anthropic)  legacy="dagster-contrib-anthropic" ;;
  dagster-chroma)     legacy="dagster-contrib-chroma" ;;
  dagster-gemini)     legacy="dagster-contrib-gemini" ;;
  dagster-modal)      legacy="dagster-contrib-modal" ;;
  dagster-notdiamond) legacy="dagster-contrib-notdiamond" ;;
  dagster-weaviate)   legacy="dagster-contrib-weaviate" ;;
  *)                  legacy="" ;;
esac

# Paths to search for commits, and tag globs to search for releases.
paths=("$PACKAGE_DIR")
tag_globs=("refs/tags/${PACKAGE//-/_}-*")
if [ -n "$legacy" ]; then
  paths+=("libraries/${legacy}")
  tag_globs+=("refs/tags/${legacy//-/_}-*")
fi

# Releases ordered by tag date, not by version number. dagster-notdiamond went
# 0.1.2 -> 0.1.3 -> 0.0.3 -> 0.0.4 -> 0.0.5 (see issue #340), so sorting by
# version would compute every commit range against the wrong predecessor.
# Where the same version appears under both the old and new prefix it is the
# same release re-tagged, so the first one seen wins.
tags=()
versions=()
seen=""
while IFS=' ' read -r _ tag; do
  version="${tag##*-}"
  case "$version" in
    [0-9]*.[0-9]*.[0-9]*) ;;
    *) continue ;;
  esac
  case " $seen " in
    *" $version "*) continue ;;
  esac
  seen="$seen $version"
  tags+=("$tag")
  versions+=("$version")
done < <(git for-each-ref --sort=creatordate --format='%(creatordate:unix) %(refname:short)' "${tag_globs[@]}")

if [ "${#versions[@]}" -eq 0 ]; then
  echo "ERROR: No release tags found for ${PACKAGE}" >&2
  echo "Did you fetch tags?  git fetch origin --tags" >&2
  exit 1
fi

echo "# Changelog"
echo ""
echo "All notable changes to this integration will be documented in this file."
echo ""
echo "The format follows [Keep a Changelog](https://keepachangelog.com/en/1.0.0/)."
echo ""
echo "## [Unreleased]"

# Walk newest -> oldest so the changelog reads newest first, while comparing
# each release against the one that preceded it chronologically.
for (( i = ${#versions[@]} - 1; i >= 0; i-- )); do
  version="${versions[$i]}"
  tag="${tags[$i]}"
  date=$(git log -1 --format=%ad --date=short "$tag")

  if [ "$i" -gt 0 ]; then
    range="${tags[$((i - 1))]}..${tag}"
  else
    range="$tag"   # first release: everything up to that tag
  fi

  echo ""
  echo "## [${version}] - ${date}"
  echo ""

  entries=$(git log "$range" --no-merges --format='- %s' -- "${paths[@]}" \
    | grep -viE '^- (\[[^]]*\] *)?(bump |release |prep(are)? release|version bump)' \
    | sed -E 's/^- \[[^]]*\] */- /' || true)

  if [ -n "$entries" ]; then
    echo "$entries"
  else
    echo "- See the git history for the changes in this release."
  fi
done
