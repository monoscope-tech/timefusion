#!/usr/bin/env bash
set -euo pipefail

readonly repo_root="$(git rev-parse --show-toplevel)"
readonly state_dir="${RS_DISTILL_STATE_DIR:-$(git -C "$repo_root" rev-parse --git-path rs-distill)}"
readonly completed="$state_dir/completed"
readonly model="${RS_DISTILL_MODEL:-gpt-5.6-terra}"

mkdir -p "$state_dir"
touch "$completed"

if [[ $# -gt 0 ]]; then
  files=("$@")
else
  files=()
  while IFS= read -r -d '' file; do files+=("$file"); done \
    < <(git -C "$repo_root" ls-files -z '*.rs' ':!vendor/**')
fi

for file in "${files[@]}"; do
  file="${file#./}"
  [[ -f "$repo_root/$file" ]] || { echo "missing Rust file: $file" >&2; exit 1; }
  grep -Fxq "$file" "$completed" && continue

  echo "rs-distill: $file"
  codex exec --ephemeral --dangerously-bypass-approvals-and-sandbox \
    --model "$model" --cd "$repo_root" \
    "Use the rs-distill skill to review and edit only $file. Remove boilerplate, duplication, dead code, and comments that restate code. Keep short comments that explain non-obvious invariants or tradeoffs. Prefer clear expressions, iterators, immutable data, existing helpers, and standard derives/combinators. Preserve behavior and public contracts. Format the file, but do not compile or test: the parent workflow validates integrated batches. Do not edit generated code, other files, dependencies, or documentation. Finish only when the file is smaller or when you have verified that no safe, clear reduction exists."

  git diff --check -- "$file"
  printf '%s\n' "$file" >> "$completed"
done
