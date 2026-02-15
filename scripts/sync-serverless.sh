#!/usr/bin/env bash
#
# sync-serverless.sh
#
# Syncs the @bayoudhi/moose-lib-serverless package with the latest
# ts-moose-lib build output. Handles upstream pull, build, dist copy,
# dependency version sync, version bump, and publish.
#
# Usage:
#   ./scripts/sync-serverless.sh [OPTIONS]
#
# Options:
#   --patch           Patch version bump (default)
#   --minor           Minor version bump
#   --major           Major version bump
#   --dry-run         Do everything except publish
#   --skip-upstream   Skip git fetch/merge from upstream
#   --skip-build      Skip pnpm build (use existing dist)
#   -h, --help        Show this help message
#
# Examples:
#   ./scripts/sync-serverless.sh                          # pull, build, patch bump, publish
#   ./scripts/sync-serverless.sh --minor --dry-run        # minor bump, no publish
#   ./scripts/sync-serverless.sh --skip-upstream --patch   # skip git, patch bump, publish

set -euo pipefail

# -- Constants ----------------------------------------------------------------

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
TS_MOOSE_LIB="$REPO_ROOT/packages/ts-moose-lib"
SERVERLESS_PKG="$REPO_ROOT/packages/moose-lib-serverless"
SOURCE_DIST="$TS_MOOSE_LIB/dist"
TARGET_DIST="$SERVERLESS_PKG/dist"

# Dependencies that the serverless bundle actually uses at runtime.
# These versions are synced from ts-moose-lib's package.json.
SYNCED_DEPS=("@clickhouse/client" "csv-parse" "jose" "toml")

# -- Defaults -----------------------------------------------------------------

BUMP="patch"
DRY_RUN=false
SKIP_UPSTREAM=false
SKIP_BUILD=false

# -- Colors -------------------------------------------------------------------

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# -- Helpers ------------------------------------------------------------------

info()  { echo -e "${BLUE}[INFO]${NC}  $*"; }
ok()    { echo -e "${GREEN}[OK]${NC}    $*"; }
warn()  { echo -e "${YELLOW}[WARN]${NC}  $*"; }
error() { echo -e "${RED}[ERROR]${NC} $*" >&2; }
die()   { error "$*"; exit 1; }

usage() {
    sed -n '/^# Usage:/,/^$/p' "$0" | sed 's/^# \?//'
    sed -n '/^# Options:/,/^$/p' "$0" | sed 's/^# \?//'
    sed -n '/^# Examples:/,/^$/p' "$0" | sed 's/^# \?//'
    exit 0
}

# -- Parse arguments ----------------------------------------------------------

while [[ $# -gt 0 ]]; do
    case "$1" in
        --patch)         BUMP="patch"; shift ;;
        --minor)         BUMP="minor"; shift ;;
        --major)         BUMP="major"; shift ;;
        --dry-run)       DRY_RUN=true; shift ;;
        --skip-upstream) SKIP_UPSTREAM=true; shift ;;
        --skip-build)    SKIP_BUILD=true; shift ;;
        -h|--help)       usage ;;
        *)               die "Unknown option: $1. Use --help for usage." ;;
    esac
done

# -- Validate environment -----------------------------------------------------

cd "$REPO_ROOT"

if [[ ! -f "$TS_MOOSE_LIB/package.json" ]]; then
    die "Cannot find ts-moose-lib at $TS_MOOSE_LIB"
fi

if [[ ! -f "$SERVERLESS_PKG/package.json" ]]; then
    die "Cannot find moose-lib-serverless at $SERVERLESS_PKG"
fi

OLD_VERSION=$(node -p "require('$SERVERLESS_PKG/package.json').version")
info "Current @bayoudhi/moose-lib-serverless version: $OLD_VERSION"

# -- Step 1: Pull upstream ----------------------------------------------------

if [[ "$SKIP_UPSTREAM" == false ]]; then
    info "Step 1/7: Pulling upstream changes..."

    if ! git remote get-url upstream &>/dev/null; then
        die "No 'upstream' remote found. Add it with: git remote add upstream https://github.com/514-labs/moosestack.git"
    fi

    git fetch upstream
    git merge upstream/main --no-edit
    ok "Merged upstream/main"
else
    info "Step 1/7: Skipping upstream pull (--skip-upstream)"
fi

# -- Step 2: Build ts-moose-lib -----------------------------------------------

if [[ "$SKIP_BUILD" == false ]]; then
    info "Step 2/7: Building ts-moose-lib..."
    pnpm build --filter @514labs/moose-lib
    ok "Build complete"
else
    info "Step 2/7: Skipping build (--skip-build)"
fi

# Validate build output
if [[ ! -f "$SOURCE_DIST/serverless.js" ]]; then
    die "Build output not found: $SOURCE_DIST/serverless.js"
fi

# -- Step 3: Clean old dist ---------------------------------------------------

info "Step 3/7: Cleaning old dist files..."
rm -rf "$TARGET_DIST"
mkdir -p "$TARGET_DIST"
ok "Cleaned $TARGET_DIST"

# -- Step 4: Copy dist files --------------------------------------------------

info "Step 4/7: Copying dist files..."

# serverless bundle files
cp "$SOURCE_DIST"/serverless.js      "$TARGET_DIST/"
cp "$SOURCE_DIST"/serverless.js.map  "$TARGET_DIST/"
cp "$SOURCE_DIST"/serverless.mjs     "$TARGET_DIST/"
cp "$SOURCE_DIST"/serverless.mjs.map "$TARGET_DIST/"

# Type declarations
cp "$SOURCE_DIST"/serverless.d.ts  "$TARGET_DIST/"
cp "$SOURCE_DIST"/serverless.d.mts "$TARGET_DIST/"
cp "$SOURCE_DIST"/browserCompatible.d.ts  "$TARGET_DIST/"
cp "$SOURCE_DIST"/browserCompatible.d.mts "$TARGET_DIST/"

# Helper type declarations (hash in filename changes per build)
HELPERS_COUNT=0
for f in "$SOURCE_DIST"/helpers-*.d.ts "$SOURCE_DIST"/helpers-*.d.mts; do
    if [[ -f "$f" ]]; then
        cp "$f" "$TARGET_DIST/"
        HELPERS_COUNT=$((HELPERS_COUNT + 1))
    fi
done

if [[ "$HELPERS_COUNT" -eq 0 ]]; then
    die "No helpers-*.d.ts files found in $SOURCE_DIST — build may have changed structure"
fi

FILE_COUNT=$(ls -1 "$TARGET_DIST" | wc -l | tr -d ' ')
ok "Copied $FILE_COUNT files to $TARGET_DIST"

# -- Step 5: Sync dependency versions -----------------------------------------

info "Step 5/7: Syncing dependency versions from ts-moose-lib..."

DEPS_JSON=$(printf '%s\n' "${SYNCED_DEPS[@]}" | jq -R . | jq -s .)

SYNC_TMPFILE=$(mktemp)
trap "rm -f '$SYNC_TMPFILE'" EXIT

cat > "$SYNC_TMPFILE" <<NODESCRIPT
const fs = require("fs");

const sourceFile = "${TS_MOOSE_LIB}/package.json";
const targetFile = "${SERVERLESS_PKG}/package.json";
const depsToSync = ${DEPS_JSON};

const source = JSON.parse(fs.readFileSync(sourceFile, "utf8"));
const target = JSON.parse(fs.readFileSync(targetFile, "utf8"));

const sourceDeps = {
  ...source.dependencies,
  ...source.peerDependencies,
};

const changes = [];

for (const dep of depsToSync) {
  const sourceVersion = sourceDeps[dep];
  const targetVersion = target.dependencies?.[dep];

  if (!sourceVersion) {
    console.error("WARN: " + dep + " not found in ts-moose-lib dependencies");
    continue;
  }

  if (sourceVersion !== targetVersion) {
    if (!target.dependencies) target.dependencies = {};
    target.dependencies[dep] = sourceVersion;
    changes.push("  " + dep + ": " + (targetVersion || "(new)") + " -> " + sourceVersion);
  }
}

if (changes.length > 0) {
  fs.writeFileSync(targetFile, JSON.stringify(target, null, 2) + "\n");
  console.log(changes.join("\\n"));
} else {
  console.log("  (no changes)");
}
NODESCRIPT

node "$SYNC_TMPFILE"

ok "Dependencies synced"

# -- Step 6: Bump version -----------------------------------------------------

info "Step 6/7: Bumping version ($BUMP)..."

cd "$SERVERLESS_PKG"
NEW_VERSION=$(npm version "$BUMP" --no-git-tag-version 2>/dev/null | tr -d 'v')
cd "$REPO_ROOT"

ok "Version: $OLD_VERSION -> $NEW_VERSION"

# -- Step 7: Publish ----------------------------------------------------------

echo ""
echo "============================================"
echo "  Sync Summary"
echo "============================================"
echo "  Package:  @bayoudhi/moose-lib-serverless"
echo "  Version:  $OLD_VERSION -> $NEW_VERSION"
echo "  Bump:     $BUMP"
echo "  Files:    $FILE_COUNT files in dist/"
echo "============================================"
echo ""

if [[ "$DRY_RUN" == true ]]; then
    info "Step 7/7: Dry run — skipping publish"
    warn "To publish, run without --dry-run"
    echo ""
    info "You can also publish manually:"
    echo "  cd $SERVERLESS_PKG && npm publish --access public"
else
    info "Step 7/7: Publishing to npm..."
    cd "$SERVERLESS_PKG"
    npm publish --access public
    cd "$REPO_ROOT"
    ok "Published @bayoudhi/moose-lib-serverless@$NEW_VERSION"

    # Quick verification
    echo ""
    info "Verifying on registry..."
    sleep 2
    if npm view "@bayoudhi/moose-lib-serverless@$NEW_VERSION" version &>/dev/null 2>&1; then
        ok "Verified: @bayoudhi/moose-lib-serverless@$NEW_VERSION is live on npm"
    else
        warn "Package published but registry may need a moment to propagate"
    fi
fi

echo ""
info "Next steps:"
echo "  1. Test: npm install @bayoudhi/moose-lib-serverless@$NEW_VERSION"
echo "  2. Commit: git add -A && git commit -m 'chore: sync moose-lib-serverless to $NEW_VERSION'"
echo "  3. Push: git push origin feature/serverless-compatibility"
