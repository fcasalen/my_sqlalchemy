#!/bin/bash
set -e

# -------------------------------
# 1. Check if to skip version check
# -------------------------------
COMMIT_MSG=$(git log -1 --pretty=%B 2>/dev/null || echo "")
if echo "$COMMIT_MSG" | grep -q "\[skip-version\]"; then
    echo "⚠️ Commit marked to skip version check."
    exit 0
fi

# -------------------------------
# 2. Detect file and pattern
# -------------------------------
if [[ -f "pyproject.toml" ]]; then
    FILE="pyproject.toml"
    PATTERN="^version\s*="
elif [[ -f "setup.py" ]]; then
    FILE="setup.py"
    PATTERN="version\s*="
else
    echo "❌ No setup.py or pyproject.toml found!"
    exit 1
fi

# -------------------------------
# 3. Check if file is staged
# -------------------------------
if ! git diff --cached --name-only | grep -q "$FILE"; then
    echo "❌ Error: $FILE not found. Update the version."
    exit 1
fi

# -------------------------------
# 4. Extract old and new versions
# -------------------------------
OLD_VERSION=$(git show HEAD:$FILE 2>/dev/null | grep -E "$PATTERN" | head -1 | sed -E 's/.*version.*= *["'\'']([^"'\'']+)["'\''].*/\1/')
NEW_VERSION=$(git show :$FILE | grep -E "$PATTERN" | head -1 | sed -E 's/.*version.*= *["'\'']([^"'\'']+)["'\''].*/\1/')

if [[ -z "$OLD_VERSION" ]]; then
    echo "ℹ️ No old version found (maybe it's first commit?)."
    exit 0
fi

# -------------------------------
# 5. Compare versions
# -------------------------------
if [[ "$OLD_VERSION" == "$NEW_VERSION" ]]; then
    echo "❌ Error: version in $FILE not incremented (continues in $NEW_VERSION)."
    exit 1
fi

echo "✅ Updated version: $OLD_VERSION → $NEW_VERSION"
exit 0
