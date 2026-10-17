#!/bin/sh
set -eu

case "${1:-}" in
  identity)
    metadata="$(git var GIT_AUTHOR_IDENT)
$(git var GIT_COMMITTER_IDENT)"
    ;;
  history)
    shift
    if [ "$#" -eq 0 ]; then set -- HEAD; fi
    metadata=$(git log --format='%H%n%an <%ae>%n%cn <%ce>%n%(trailers:key=Co-authored-by,valueonly)' "$@")
    ;;
  *)
    printf '%s\n' 'Usage: check-attribution.sh identity | history [revision ...]' >&2
    exit 2
    ;;
esac

# Reject the unrelated machine-default identity, not genuine outside contributors.
if printf '%s\n' "$metadata" | LC_ALL=C grep -Eiq '(^|[[:space:]<])(hello@reila\.re|([0-9]+\+)?helloreila@users\.noreply\.github\.com)([[:space:]>]|$)|^Reila[[:space:]<]'; then
  printf '%s\n' \
    'Rejected an unrelated inherited Git identity.' \
    'For Blackscale maintainer work, configure this checkout before committing:' \
    '  git config --local user.name Blackscale' \
    '  git config --local user.email hello@blackscale.tech' >&2
  exit 1
fi
