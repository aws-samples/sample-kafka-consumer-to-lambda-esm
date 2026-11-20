#!/usr/bin/env bash
#
# Produce a submission archive containing only tracked files.
#
# Build directories must not reach a content scanner. `terraform/.terraform/producer-build/`
# holds the producer's pip-installed dependencies: 536 third-party Python files from setuptools,
# click, urllib3, dateutil, jmespath and six. Those are gitignored and are not part of this
# repository, but zipping the working directory captures them anyway, and a scanner then reports
# their subprocess calls, exec() usage and weak-SSL constants as findings against this project.
#
# A scan of the working directory reported 399 findings, of which 15 were in tracked files.
#
# `git archive` writes only what is committed, so the archive matches what a reader clones.
#
#   ./scripts/package_for_review.sh            # archive HEAD
#   ./scripts/package_for_review.sh v1.0.0     # archive a tag

set -euo pipefail

REF="${1:-HEAD}"
NAME="sample-kafka-consumer-to-lambda-esm"
OUT="${NAME}-${REF//\//-}.zip"

cd "$(dirname "${BASH_SOURCE[0]}")/.."

if [[ -n "$(git status --porcelain)" ]]; then
  echo "warning: working tree has uncommitted changes, which git archive will not include." >&2
  git status --short >&2
  echo >&2
fi

git archive --format=zip --prefix="${NAME}/" -o "../${OUT}" "$REF"

echo "wrote ../${OUT}"
echo
echo "contents:"
unzip -Z1 "../${OUT}" | sort | sed 's/^/  /'
echo
echo "file count: $(unzip -Z1 "../${OUT}" | grep -vc '/$')"

echo
echo "verifying no build artifacts, state or local config are present:"

# Patterns are matched against the entry list with grep -F, as fixed strings. An unescaped
# ".terraform/" used as a regex also matches "/terraform/", which is a legitimate directory here
# and made this check fail on a clean archive.
#
# ".tfvars" is deliberately absent from the list: prereq.auto.tfvars.example belongs in the
# archive, and a populated prereq.auto.tfvars does not. The two are distinguished below.
entries="$(unzip -Z1 "../${OUT}")"
fail=0

for pattern in '/.terraform/' 'producer-build' 'producer.zip' '.tfstate' '/.venv/' '__pycache__' '.pytest_cache' '/.ash/'; do
  if printf '%s\n' "$entries" | grep -qF -- "$pattern"; then
    echo "  FAIL: $pattern is present" >&2
    fail=1
  else
    echo "  ok: no $pattern"
  fi
done

# A tfvars file carrying real cluster identifiers must never ship. The .example must.
if printf '%s\n' "$entries" | grep -F '.tfvars' | grep -qvF '.tfvars.example'; then
  echo "  FAIL: a populated .tfvars file is present" >&2
  fail=1
else
  echo "  ok: no populated .tfvars (the .example is expected)"
fi

if [[ "$fail" -ne 0 ]]; then
  echo >&2
  echo "archive is not safe to submit" >&2
  exit 1
fi

echo
echo "archive contains only tracked files and is safe to submit for scanning."
