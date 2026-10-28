#!/usr/bin/env bash
set -euo pipefail

repo_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
cd "$repo_dir"

token_file=${GITHUB_TOKEN_FILE:-"$repo_dir/github_token"}
remote=${GITHUB_REMOTE:-origin}
branch=${GITHUB_BRANCH:-main}

if [[ ! -s "$token_file" ]]; then
    printf 'Token file is missing or empty: %s\n' "$token_file" >&2
    exit 1
fi

if git ls-files --error-unmatch -- "$token_file" >/dev/null 2>&1; then
    printf 'Refusing to continue: token file is tracked by Git.\n' >&2
    exit 1
fi

remote_url=$(git remote get-url "$remote")
case "$remote_url" in
    https://github.com/*)
        repository=${remote_url#https://github.com/}
        github_user=${repository%%/*}
        ;;
    *)
        printf 'Remote %s must use an HTTPS GitHub URL, got: %s\n' \
            "$remote" "$remote_url" >&2
        exit 1
        ;;
esac

# Strip only line endings introduced by the editor. Never print the token.
github_token=$(tr -d '\r\n' < "$token_file")
if [[ -z "$github_token" ]]; then
    printf 'Token file contains no token.\n' >&2
    exit 1
fi

chmod 600 "$token_file"
askpass=$(mktemp)
cleanup() {
    unset GITHUB_TOKEN GITHUB_USERNAME GIT_ASKPASS GIT_TERMINAL_PROMPT
    rm -f -- "$askpass"
}
trap cleanup EXIT HUP INT TERM

cat > "$askpass" <<'ASKPASS'
#!/bin/sh
case "$1" in
    *Username*) printf '%s\n' "$GITHUB_USERNAME" ;;
    *Password*) printf '%s\n' "$GITHUB_TOKEN" ;;
    *) exit 1 ;;
esac
ASKPASS
chmod 700 "$askpass"

export GITHUB_USERNAME=$github_user
export GITHUB_TOKEN=$github_token
export GIT_ASKPASS=$askpass
export GIT_TERMINAL_PROMPT=0
unset github_token

printf 'Pushing %s to %s (%s)...\n' "$branch" "$remote" "$remote_url"
timeout "${GITHUB_PUSH_TIMEOUT:-180}" \
    git -c credential.helper= -c http.version=HTTP/1.1 \
    push -u "$remote" "$branch"

local_head=$(git rev-parse "$branch")
remote_head=$(git ls-remote "$remote" "refs/heads/$branch" | awk '{print $1}')
if [[ "$local_head" != "$remote_head" ]]; then
    printf 'Push returned successfully, but remote verification did not match.\n' >&2
    exit 1
fi

printf 'Push verified: %s\n' "$local_head"
