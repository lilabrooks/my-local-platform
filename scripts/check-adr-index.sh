#!/usr/bin/env bash
# Assert that README's Design and evidence section indexes every ADR exactly
# once with its declared status. Both sides are discovered, so adding an ADR or
# changing its status never requires changing an expected value in this check.
set -euo pipefail

repo_root="${1:-$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)}"
adr_dir="${ADR_INDEX_ADR_DIR:-$repo_root/docs/adr}"
readme="${ADR_INDEX_README:-$repo_root/README.md}"

if [ ! -d "$adr_dir" ]; then
  echo "ADR directory does not exist: $adr_dir" >&2
  exit 1
fi
if [ ! -r "$readme" ]; then
  echo "README is not readable: $readme" >&2
  exit 1
fi

expected=$(
  find "$adr_dir" -maxdepth 1 -type f \
    -name '[0-9][0-9][0-9][0-9]-*.md' -print \
    | while IFS= read -r path; do
        printf 'docs/adr/%s\n' "${path##*/}"
      done \
    | LC_ALL=C sort
)

if [ -z "$expected" ]; then
  echo "no numbered ADR files found in $adr_dir" >&2
  exit 1
fi

if ! section=$(
  awk '
    $0 == "## Design and evidence" { found = 1; inside = 1; next }
    inside && /^## / { exit }
    inside { print }
    END { if (!found) exit 2 }
  ' "$readme"
); then
  echo "$readme has no '## Design and evidence' section" >&2
  exit 1
fi

# Extract only numbered ADR Markdown links. Fragments are allowed and removed
# before comparison because the file, rather than one heading, is the index
# entry. `|| true` keeps an empty link set available for the missing-file report.
listed=$(
  printf '%s\n' "$section" \
    | grep -Eo '\(docs/adr/[0-9]{4}-[^)#?]+\.md(#[^)]*)?\)' \
    | sed -E 's/^\(([^#)]+)(#[^)]*)?\)$/\1/' \
    | LC_ALL=C sort \
    || true
)

unique_listed=$(printf '%s\n' "$listed" | sed '/^$/d' | LC_ALL=C sort -u)
duplicates=$(printf '%s\n' "$listed" | sed '/^$/d' | LC_ALL=C uniq -d)
missing=$(comm -23 \
  <(printf '%s\n' "$expected") \
  <(printf '%s\n' "$unique_listed"))
stale=$(comm -13 \
  <(printf '%s\n' "$expected") \
  <(printf '%s\n' "$unique_listed"))

failed=0
if [ -n "$missing" ]; then
  echo "README's Design and evidence section is missing ADR links:" >&2
  printf '%s\n' "$missing" | sed 's/^/  /' >&2
  failed=1
fi
if [ -n "$stale" ]; then
  echo "README's Design and evidence section has stale ADR links:" >&2
  printf '%s\n' "$stale" | sed 's/^/  /' >&2
  failed=1
fi
if [ -n "$duplicates" ]; then
  echo "README's Design and evidence section lists ADRs more than once:" >&2
  printf '%s\n' "$duplicates" | sed 's/^/  /' >&2
  failed=1
fi

# Each ADR owns its status. The README exposes that status to readers, and this
# comparison makes a change to either side fail until both describe the same
# state. No allowed-status list lives here; the ADR's declared value is the
# source.
status_values=""
while IFS= read -r path; do
  adr_file="$adr_dir/${path##*/}"
  status_lines=$(grep -E '^Status:' "$adr_file" || true)
  status_count=$(printf '%s\n' "$status_lines" | awk 'NF { n++ } END { print n + 0 }')
  if [ "$status_count" -ne 1 ]; then
    echo "$path has $status_count 'Status:' lines; want exactly 1" >&2
    failed=1
    continue
  fi

  adr_status=$(printf '%s\n' "${status_lines#Status:}" \
    | sed 's/^[[:space:]]*//; s/[[:space:]]*$//')
  if [ -z "$adr_status" ]; then
    echo "$path has an empty Status value" >&2
    failed=1
    continue
  fi
  status_values="${status_values}${adr_status}"$'\n'

  readme_lines=$(printf '%s\n' "$section" | grep -F "]($path" || true)
  readme_line_count=$(printf '%s\n' "$readme_lines" \
    | awk 'NF { n++ } END { print n + 0 }')
  if [ "$readme_line_count" -ne 1 ]; then
    # Membership and duplicate errors above already name this path.
    continue
  fi

  readme_status=$(printf '%s\n' "$readme_lines" \
    | sed -nE 's/^\|.*\]\([^)]*\)[[:space:]]*\|[[:space:]]*([^|]+)[[:space:]]*\|[[:space:]]*$/\1/p' \
    | sed 's/^[[:space:]]*//; s/[[:space:]]*$//')
  if [ -z "$readme_status" ]; then
    echo "$readme does not state the status of $path in a table row" >&2
    failed=1
  elif [ "$readme_status" != "$adr_status" ]; then
    echo "$readme status for $path is '$readme_status'; ADR declares '$adr_status'" >&2
    failed=1
  fi
done <<< "$expected"

if [ "$failed" -ne 0 ]; then
  exit 1
fi

count=$(printf '%s\n' "$expected" | awk 'NF { n++ } END { print n + 0 }')
status_summary=$(printf '%s' "$status_values" \
  | sed '/^$/d' \
  | LC_ALL=C sort \
  | uniq -c \
  | awk '{ count = $1; $1 = ""; sub(/^ /, "");
           printf "%s%s: %d", separator, $0, count; separator = ", " }')
printf 'README ADR index covers all %d discovered ADR files (%s)\n' \
  "$count" "$status_summary"
