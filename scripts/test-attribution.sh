#!/bin/sh
set -eu
root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
fixture=$(mktemp -d)
trap 'rm -rf "$fixture"' EXIT HUP INT TERM
export GIT_CONFIG_NOSYSTEM=1 GIT_CONFIG_GLOBAL=/dev/null
unset GIT_AUTHOR_NAME GIT_AUTHOR_EMAIL GIT_COMMITTER_NAME GIT_COMMITTER_EMAIL
git init -q "$fixture"
cd "$fixture"
git config user.name Blackscale
git config user.email hello@blackscale.tech
mkdir scripts
cp "$root/scripts/check-attribution.sh" scripts/
git config core.hooksPath "$root/.githooks"

reject() {
  if "$@" >"$fixture/output" 2>&1; then
    printf 'Expected rejection: %s\n' "$*" >&2
    exit 1
  fi
  grep -q 'Rejected an unrelated inherited Git identity' "$fixture/output"
}

sh scripts/check-attribution.sh identity
reject env GIT_AUTHOR_NAME=Reila GIT_AUTHOR_EMAIL=hello@reila.re sh scripts/check-attribution.sh identity
reject env GIT_COMMITTER_EMAIL=hello@reila.re sh scripts/check-attribution.sh identity
reject env GIT_AUTHOR_EMAIL=123+helloreila@users.noreply.github.com sh scripts/check-attribution.sh identity
env GIT_AUTHOR_NAME=Contributor GIT_AUTHOR_EMAIL=contributor@example.org sh scripts/check-attribution.sh identity

git -c commit.gpgsign=false commit -q --allow-empty -m 'Synthetic accepted commit'
sh scripts/check-attribution.sh history HEAD
reject env GIT_AUTHOR_NAME=Reila GIT_AUTHOR_EMAIL=hello@reila.re git -c commit.gpgsign=false commit -q --allow-empty -m 'Synthetic hook rejection'
test "$(git rev-list --count HEAD)" = 1

# Model an inherited identity arriving from another machine or a bypassed hook.
git -c core.hooksPath=/dev/null -c commit.gpgsign=false -c user.email=hello@reila.re commit -q --allow-empty -m 'Synthetic bad author'
reject sh scripts/check-attribution.sh history HEAD
reject sh scripts/check-attribution.sh history HEAD^..HEAD
if printf 'refs/heads/test %s refs/heads/test %s\n' "$(git rev-parse HEAD)" 0000000000000000000000000000000000000000 | sh "$root/.githooks/pre-push" >"$fixture/output" 2>&1; then
  printf '%s\n' 'Expected pre-push rejection' >&2
  exit 1
fi
grep -q 'Rejected an unrelated inherited Git identity' "$fixture/output"

git -c core.hooksPath=/dev/null -c commit.gpgsign=false commit -q --allow-empty -m 'Synthetic bad trailer' -m 'Co-authored-by: Reila <hello@reila.re>'
reject sh scripts/check-attribution.sh history HEAD^..HEAD
git -c core.hooksPath=/dev/null -c commit.gpgsign=false commit -q --allow-empty -m 'Synthetic outside contributor' --author='Contributor <contributor@example.org>'
sh scripts/check-attribution.sh history HEAD^..HEAD
printf '%s\n' 'Attribution guard and hooks passed, including negative cases.'
