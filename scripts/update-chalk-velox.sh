#!/bin/bash
# Updates the chalk-ai/velox fork by syncing main from upstream
# (facebookincubator/velox) and creating a merge branch for chalk-main.
#
# Usage: ./scripts/update-velox.sh

set -euo pipefail

UPSTREAM_REMOTE="upstream"
UPSTREAM_URL="git@github.com:facebookincubator/velox.git"
DATE=$(date +%Y-%m-%d)
UPDATE_BRANCH="update-velox-${DATE}"

red()   { printf "\033[31m%s\033[0m\n" "$*"; }
green() { printf "\033[32m%s\033[0m\n" "$*"; }
bold()  { printf "\033[1m%s\033[0m\n" "$*"; }

# Bail out on dirty working tree.
if ! git diff --quiet || ! git diff --cached --quiet; then
  red "Error: working tree is dirty. Please commit or stash changes first."
  exit 1
fi

# ── Step 1: Ensure upstream remote exists ────────────────────────────────────
bold "Step 1: Ensuring upstream remote is configured..."
if ! git remote get-url "${UPSTREAM_REMOTE}" &>/dev/null; then
  echo "  Adding remote '${UPSTREAM_REMOTE}' -> ${UPSTREAM_URL}"
  git remote add "${UPSTREAM_REMOTE}" "${UPSTREAM_URL}"
elif [ "$(git remote get-url ${UPSTREAM_REMOTE})" != "${UPSTREAM_URL}" ]; then
  echo "  Updating remote '${UPSTREAM_REMOTE}' to ${UPSTREAM_URL}"
  git remote set-url "${UPSTREAM_REMOTE}" "${UPSTREAM_URL}"
else
  echo "  Remote '${UPSTREAM_REMOTE}' already configured."
fi

# ── Step 2: Sync main with upstream ──────────────────────────────────────────
bold "Step 2: Syncing main with upstream..."
echo "  Fetching from ${UPSTREAM_REMOTE}..."
git fetch "${UPSTREAM_REMOTE}" main

ORIGINAL_BRANCH=$(git symbolic-ref --short HEAD 2>/dev/null || git rev-parse --short HEAD)

echo "  Checking out main..."
git checkout main

LOCAL_MAIN=$(git rev-parse main)
UPSTREAM_MAIN=$(git rev-parse "${UPSTREAM_REMOTE}/main")

if [ "${LOCAL_MAIN}" = "${UPSTREAM_MAIN}" ]; then
  green "  main is already up to date with ${UPSTREAM_REMOTE}/main."
else
  echo "  Fast-forwarding main to ${UPSTREAM_REMOTE}/main..."
  if ! git merge --ff-only "${UPSTREAM_REMOTE}/main"; then
    red "Error: main cannot be fast-forwarded to ${UPSTREAM_REMOTE}/main."
    red "This means main has diverged from upstream. Please resolve manually."
    git checkout "${ORIGINAL_BRANCH}"
    exit 1
  fi
  green "  main updated: $(git log --oneline -1)"
fi

echo "  Pushing main to origin..."
git push origin main

# ── Step 3: Create update branch from chalk-main ────────────────────────────
bold "Step 3: Creating branch '${UPDATE_BRANCH}' from chalk-main..."

if git rev-parse --verify "${UPDATE_BRANCH}" &>/dev/null; then
  red "Error: branch '${UPDATE_BRANCH}' already exists."
  red "If you need to retry, delete it first: git branch -D ${UPDATE_BRANCH}"
  git checkout "${ORIGINAL_BRANCH}"
  exit 1
fi

git checkout --no-track -b "${UPDATE_BRANCH}" origin/chalk-main
green "  Created '${UPDATE_BRANCH}' at $(git log --oneline -1)"

# ── Step 4: Merge main into the update branch ───────────────────────────────
bold "Step 4: Merging main into ${UPDATE_BRANCH}..."
echo ""

if git merge main --no-edit; then
  green "Merge completed cleanly!"
else
  echo ""
  bold "There are merge conflicts that need manual resolution."
  echo ""
  echo "  Conflicting files:"
  git diff --name-only --diff-filter=U | sed 's/^/    /'
  echo ""
  echo "  Resolve each conflict, then:"
  echo "    git add <resolved-file>"
  echo "    git merge --continue"
  echo ""
fi

# ── Step 5: Next steps ──────────────────────────────────────────────────────
echo ""
bold "═══════════════════════════════════════════════════════"
bold " Next steps"
bold "═══════════════════════════════════════════════════════"
echo ""
echo "  1. If there were conflicts, resolve them and complete the merge."
echo ""
echo "  2. Push the branch and create a PR:"
echo "       git push -u origin ${UPDATE_BRANCH}"
echo "       gh pr create --base chalk-main --title 'Update Velox ${DATE}'"
echo ""
echo "  3. After the PR is merged, update the commit hash in libchalk"
echo "     to point to the new chalk-main HEAD."
echo ""
