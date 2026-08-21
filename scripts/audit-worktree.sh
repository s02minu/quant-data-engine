#!/usr/bin/env bash
#
# Create a disposable git worktree for an external auditor (Codex, a reviewer, a
# second model) to work in.
#
# WHY a worktree rather than just "please be careful":
#   read-only is a property of the WORKFLOW, not of the tool. An auditor pointed at
#   the primary checkout can edit tracked files, and a well-meaning "I fixed it while
#   I was in there" is indistinguishable from a mistake until you read the diff. A
#   worktree gives the audit its own branch and its own directory: everything it
#   touches is visible as a diff against a throwaway ref, and deleting the directory
#   undoes all of it.
#
# WHAT THIS DOES NOT PROTECT:
#   the filesystem is the cheap half. The irreversible action in this platform is
#   PUBLISHING to the public R2 bucket. That is gated by credentials, not by paths —
#   see the notes printed at the end.
#
# Usage:
#   bash scripts/audit-worktree.sh                 # create, code only
#   bash scripts/audit-worktree.sh --with-secrets  # also copy READ-ONLY creds
#   bash scripts/audit-worktree.sh --remove <path> # tear one down
#   bash scripts/audit-worktree.sh --list

set -euo pipefail
cd "$(dirname "$0")/.."
REPO_ROOT="$(pwd)"
REPO_NAME="$(basename "$REPO_ROOT")"

case "${1:-}" in
  --list)
    # --porcelain, because this repo lives under a path containing spaces and the
    # human-readable form cannot be parsed by whitespace without splitting it.
    git worktree list --porcelain | grep "^worktree " | sed 's/^worktree /  /'
    exit 0
    ;;
  --remove)
    TARGET="${2:?usage: --remove <worktree-path>}"
    # Refuse to remove the primary checkout, whatever was passed.
    if [ "$(cd "$TARGET" && pwd)" = "$REPO_ROOT" ]; then
      echo "refusing to remove the primary checkout" >&2
      exit 1
    fi
    BRANCH="$(git -C "$TARGET" rev-parse --abbrev-ref HEAD 2>/dev/null || echo '')"
    git worktree remove --force "$TARGET"
    [ -n "$BRANCH" ] && git branch -D "$BRANCH" 2>/dev/null || true
    git worktree prune
    echo "removed $TARGET (branch $BRANCH)"
    exit 0
    ;;
esac

WITH_SECRETS=0
[ "${1:-}" = "--with-secrets" ] && WITH_SECRETS=1

# Uncommitted work is INVISIBLE to a worktree — it branches from a commit. Auditing
# a stale tree and reporting on code that no longer exists is worse than not
# auditing at all, so this refuses rather than producing a misleading review.
if [ -n "$(git status --porcelain)" ]; then
  echo "ERROR: working tree is dirty." >&2
  echo "  A worktree branches from a commit, so an auditor would review the LAST" >&2
  echo "  COMMITTED state and silently miss everything below. Commit first." >&2
  git status --short >&2
  exit 1
fi

STAMP="$(date -u +%Y%m%dT%H%M%SZ)"
BRANCH="audit/$STAMP"
# Sibling directory, deliberately OUTSIDE the repo: this project's tooling globs
# recursively (the lake, dbt, the bronze consolidation), and a worktree nested inside
# the repo gets swept into those globs as if it were data.
DEST="$REPO_ROOT/../${REPO_NAME}-audit-$STAMP"

git worktree add -b "$BRANCH" "$DEST" HEAD >/dev/null
echo "worktree : $DEST"
echo "branch   : $BRANCH  (from $(git rev-parse --short HEAD))"

if [ "$WITH_SECRETS" = "1" ]; then
  mkdir -p "$DEST/secrets"
  for f in secrets/r2-read.env secrets/fred.env; do
    [ -f "$f" ] && cp "$f" "$DEST/secrets/" && echo "copied   : $f"
  done
  # Never the write credentials. They live in secrets-infra/ (VPS-only, deliberately
  # NOT mounted into containers) and are what publishing authenticates with.
  #
  # BOTH paths are checked: this used to test only secrets/r2.env, so when the file
  # moved the reassurance simply stopped printing — a safety message that silently
  # disappears is worse than none, because its absence reads as "nothing to withhold".
  for wf in secrets-infra/r2.env secrets/r2.env; do
    if [ -f "$wf" ]; then
      echo "WITHHELD : $wf (write credentials) — not copied, by design"
    fi
  done
fi

cat <<NOTES

--- what the auditor can and cannot do ---
  CAN   read all code, run pytest/ruff/mypy, build the site, query the PUBLIC lake
        over plain HTTPS (no credentials needed — that is the whole serving model)
  CAN   edit freely inside $DEST; review with:
          git -C "$DEST" diff
  CANNOT publish: the read credentials use QDE_R2_READ_KEY_ID / QDE_R2_READ_SECRET,
        while publish_public reads QDE_R2_ACCESS_KEY_ID / QDE_R2_SECRET_ACCESS_KEY.
        Different names, so the publish path finds no credentials and fails closed.
  DO NOT hand over VPS SSH or secrets-infra/r2.env. Point at the public URL —
        a stranger can verify this lake with no credentials, so an auditor needs
        nothing more than a stranger does.

--- tear down when finished ---
  bash scripts/audit-worktree.sh --remove "$DEST"
NOTES
