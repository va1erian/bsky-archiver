---
tracker:
  kind: github
  provider:
    repo: va1erian/bsky-archiver
    token: $SYMPHONY_GITHUB_TOKEN
    closed_state: done
    active_state_labels:
      todo: "state:todo"
      in progress: "state:in-progress"
  active_states: [todo, "in progress"]
  terminal_states: [done]

polling:
  interval_ms: 30000

workspace:
  root: ./.workspaces
  docker:
    enabled: true
    image: bsky-archiver-agent:latest
    network: bridge
    user: "1000:1000"
    mount_claude_credentials: false

repo:
  url: https://github.com/va1erian/bsky-archiver.git
  default_branch: main
  token: $SYMPHONY_GITHUB_TOKEN
  pull_request: true

agent:
  backend: claude
  max_concurrent_agents: 2
  max_turns: 40
  max_retry_backoff_ms: 300000

claude:
  command: claude
  permission_mode: bypassPermissions
  api_key: $ANTHROPIC_API_KEY
  turn_timeout_ms: 3600000
  stall_timeout_ms: 300000
---
You are working on **{{ issue.identifier }}: {{ issue.title }}** in the
`va1erian/bsky-archiver` Rust project (a self-hosted daemon + web UI that archives a
Bluesky account's media posts, likes, and bookmarks). This project is already built
and production-ready as of the last completed milestone -- read `README.md` and skim
the existing source tree before writing anything; you're extending or fixing working,
tested code, not starting from scratch.

{{ issue.description }}

(Attempt: {{ attempt | default: "first attempt" }})

## Mechanics

This workspace is a git clone on its own branch (`issue-{{ issue.identifier }}`) off
`main`. The harness automatically commits and pushes your work back after every turn
as a safety net, but **do not rely on it as your actual completion gate** -- it runs
after your turn ends. Before you consider yourself done, push your own work yourself
and confirm it actually succeeded:

```
git add -A && git commit -m "<short description>" --allow-empty-message -q
git push origin "HEAD:refs/heads/$(git rev-parse --abbrev-ref HEAD)"
```

Check the push command's own output/exit status. If it fails, do not proceed --
investigate (usually `git pull --ff-only` first, then retry) or explain the problem
in your final message instead.

## Submitting your work

Once your branch is pushed and you're satisfied with the change, call the
`open_pull_request` tool with a `title` and a `body` describing what changed and why.
Include a line `Closes #{{ issue.identifier }}` in the body -- the tracker issue
closes automatically when a human reviews and merges the PR. **Do not call
`update_issue_state` to close this issue yourself** -- in this workflow, "done" means
merged, not "I'm finished writing code," and that decision belongs to whoever reviews
the PR. If you call `open_pull_request` again later (e.g. after pushing more work in
a retry), it updates the existing PR in place rather than opening a duplicate.

If you get meaningfully stuck on something outside this ticket's scope, say so
clearly in your final message instead of opening a PR for incomplete work.

## Quality bar

Real error handling (no `unwrap()`/`expect()` outside tests or genuinely unreachable
cases, each justified with a comment), structured logging via `tracing`, and
meaningful automated tests for anything you add or change (unit tests for logic,
integration tests for anything touching the network/filesystem/DB, mocks only --
never hit the real Bluesky API from a test). A ticket is not done until `cargo fmt
--check`, `cargo clippy --all-targets -- -D warnings`, and `cargo test` all pass
cleanly.
