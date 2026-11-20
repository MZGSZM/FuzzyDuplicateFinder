#!/usr/bin/env bash
# Build release notes for a tag from the git history since the previous tag.
#
#   release-notes.sh generate TAG [REPO_URL]    print notes for TAG
#   release-notes.sh merge EXISTING NOTES       print EXISTING with its old
#                                               generated block replaced by NOTES
#
# The generated block sits between two HTML comment markers, so rerunning the
# release workflow on the same tag replaces it instead of stacking copies, and
# anything typed into the release body by hand is kept.

set -euo pipefail
export LC_ALL=C

TAB=$(printf '\t')
START_MARK='<!-- auto-changelog:start -->'
END_MARK='<!-- auto-changelog:end -->'

die() {
    printf 'release-notes: %s\n' "$*" >&2
    exit 1
}

need() {
    local missing=() cmd
    for cmd in "$@"; do
        command -v "$cmd" >/dev/null 2>&1 || missing+=("$cmd")
    done
    if [ "${#missing[@]}" -gt 0 ]; then
        die "missing required commands: ${missing[*]}"
    fi
}

# Bucket for a commit subject, by its first word.
classify() {
    local first
    first=$(printf '%s\n' "$1" | awk '{ print tolower($1) }')
    case "$first" in
        add|adds|added|implement|implements|implemented|introduce|introduces|new|feat|feat:|create|creates)
            printf 'Added\n' ;;
        fix|fixes|fixed|correct|corrects|resolve|resolves|repair|patch|fix:)
            printf 'Fixed\n' ;;
        remove|removes|removed|delete|deletes|deleted|drop|drops|deprecate)
            printf 'Removed\n' ;;
        *)
            printf 'Changed\n' ;;
    esac
}

# Commits made through GitHub's web editor or uploader say nothing useful;
# the file list below covers them.
is_noise() {
    case "$1" in
        "Add files via upload"|"Create "*|"Update "*|"Delete "*|"Rename "*) return 0 ;;
    esac
    return 1
}

# First entry of changes.md at TAG: from the first "YYYY.MM.DD" heading up to
# the next one.
changes_entry() {
    git show "$1:changes.md" 2>/dev/null | awk '
        /^[0-9][0-9][0-9][0-9]\.[0-9][0-9]\.[0-9][0-9]/ { n++ }
        n == 1 { print }
        n > 1 { exit }
    ' || true
}

generate() {
    local tag="$1" repo_url="${2:-}" prev range section subject hash line
    git rev-parse -q --verify "refs/tags/$tag" >/dev/null || die "no such tag: $tag"
    prev=$(git describe --tags --abbrev=0 "$tag^" 2>/dev/null || true)
    if [ -n "$prev" ]; then
        range="$prev..$tag"
    else
        range="$tag"
    fi

    printf '%s\n' "$START_MARK"
    if [ -n "$prev" ]; then
        printf '## Changes since %s\n\n' "$prev"
    else
        printf '## Changes in %s\n\n' "$tag"
    fi

    local entry
    entry=$(changes_entry "$tag")
    if [ -n "$entry" ]; then
        printf '### Summary\n\n%s\n\n' "$entry"
    fi

    local tmp
    tmp=$(mktemp "${TMPDIR:-/tmp}/relnotes.XXXXXX")
    # shellcheck disable=SC2064  # expand now: tmp is local
    trap "rm -f -- '$tmp'" EXIT

    while IFS="$TAB" read -r hash subject; do
        [ -n "$hash" ] || continue
        if is_noise "$subject"; then
            continue
        fi
        printf '%s\t%s\t%s\n' "$(classify "$subject")" "$hash" "$subject" >>"$tmp"
    done < <(git log --no-merges --format='%h%x09%s' "$range")

    if [ -s "$tmp" ]; then
        printf '### Commits\n\n'
        for section in Added Changed Fixed Removed; do
            if grep -q "^$section$TAB" "$tmp"; then
                printf '**%s**\n\n' "$section"
                while IFS="$TAB" read -r _ hash subject; do
                    printf -- '- %s (%s)\n' "$subject" "$hash"
                done < <(grep "^$section$TAB" "$tmp")
                printf '\n'
            fi
        done
    fi

    # Merged pull requests: "Merge pull request #N from owner/branch" with
    # the PR title as the first line of the body.
    local prs
    prs=$(git log --merges --format='%s%x09%b' "$range" | awk -F '\t' '
        $1 ~ /^Merge pull request #[0-9]+/ {
            split($1, words, " ")
            title = $2
            sub(/[\r\n].*/, "", title)
            if (title == "") title = $1
            printf "- %s %s\n", words[4], title
        }
    ')
    if [ -n "$prs" ]; then
        printf '### Pull requests\n\n%s\n\n' "$prs"
    fi

    if [ -n "$prev" ]; then
        local stat
        stat=$(git diff --shortstat "$prev" "$tag" | sed 's/^ *//')
        if [ -n "$stat" ]; then
            printf '### Files\n\n%s.\n\n<details><summary>File list</summary>\n\n' "$stat"
            # Backticks below are literal Markdown, not command substitution.
            # shellcheck disable=SC2016
            while IFS="$TAB" read -r line subject; do
                case "$line" in
                    A) printf -- '- added `%s`\n' "$subject" ;;
                    D) printf -- '- removed `%s`\n' "$subject" ;;
                    R*) printf -- '- renamed `%s`\n' "$subject" ;;
                    *) printf -- '- changed `%s`\n' "$subject" ;;
                esac
            done < <(git -c core.quotepath=off diff --name-status "$prev" "$tag" | cut -f1,2)
            printf '\n</details>\n\n'
        fi
        if [ -n "$repo_url" ]; then
            printf '**Full diff:** %s/compare/%s...%s\n\n' "$repo_url" "$prev" "$tag"
        fi
    fi
    printf '%s\n' "$END_MARK"
}

merge() {
    local existing="$1" notes="$2" kept
    [ -f "$existing" ] || die "not a file: $existing"
    [ -f "$notes" ] || die "not a file: $notes"
    # Drop the previous generated block, collapse blank runs, trim the ends.
    kept=$(awk -v start="$START_MARK" -v end="$END_MARK" '
        $0 == start { skip = 1 }
        !skip {
            if (NF) { if (blank && seen) print ""; blank = 0; seen = 1; print }
            else { blank = 1 }
        }
        $0 == end { skip = 0 }
    ' "$existing")
    if [ -n "$kept" ]; then
        printf '%s\n\n' "$kept"
    fi
    cat -- "$notes"
}

main() {
    need git awk sed grep cut mktemp
    local cmd="${1:-}"
    case "$cmd" in
        generate)
            [ "$#" -ge 2 ] || die "usage: $0 generate TAG [REPO_URL]"
            generate "$2" "${3:-}"
            ;;
        merge)
            [ "$#" -eq 3 ] || die "usage: $0 merge EXISTING NOTES"
            merge "$2" "$3"
            ;;
        *)
            die "usage: $0 generate TAG [REPO_URL] | merge EXISTING NOTES"
            ;;
    esac
}

main "$@"
