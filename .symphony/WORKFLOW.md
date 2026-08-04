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
      in review: "state:in-review"
  active_states: [todo, "in progress"]
  terminal_states: [done]

swebot:
  enabled: true
  backend: opencode
  command: opencode
  token: $SWEBOT_GITHUB_TOKEN
  review:
    enabled: true
  chat:
    enabled: true
    connectors: [web]          # interactive connectors: 'web' is the chat UI
    poll_interval_ms: 1000     # how often the worker looks for new messages
    remote_poll_interval_ms: 60000
    max_concurrent_replies: 2  # answers per processing cycle (1-2 is plenty)
    auto_create_issue: true    # file a finished draft immediately (default)
    first_text_deadline_ms: 5000

polling:
  interval_ms: 10000

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
  evidence: true

agent:
  backend: opencode
  max_concurrent_agents: 2
  max_turns: 40
  max_retry_backoff_ms: 300000

opencode:
  command: opencode
  auto_approve: true
  model: fireworks-ai/accounts/fireworks/models/qwen3p7-plus
  api_key: $FIREWORKS_API_KEY
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
`main`. Before doing anything else, run `git status`. The harness rebases this branch
onto `main` before every turn to catch drift early; if that hit a real conflict, you
are looking at it right now -- `git status` will say "rebasing" and any conflicted
file will have `<<<<<<<`/`=======`/`>>>>>>>` markers in it. Resolve it first: edit each
conflicted file to the correct final content (removing the markers), `git add` it,
then `git rebase --continue` -- repeat until it reports the rebase is finished. Only
then move on to the actual ticket work below. (If the conflict is unresolvable or
clearly outside this ticket's scope, `git rebase --abort` and explain why in your
final message instead of guessing.)

The harness automatically commits and pushes your work back after every turn
as a safety net, but **do not rely on it as your actual completion gate** -- it runs
after your turn ends. Before you consider yourself done, push your own work yourself
and confirm it actually succeeded:

```
git add -A && git commit -m "<short description>" --allow-empty-message -q
git push --force-with-lease origin "HEAD:refs/heads/$(git rev-parse --abbrev-ref HEAD)"
```

`--force-with-lease`, not a plain push: if you resolved a rebase conflict above, your
branch's history was rewritten, and a plain push would be rejected as non-fast-forward.
Check the push command's own output/exit status. If it fails, do not proceed --
investigate (usually `git pull --ff-only` first, then retry) or explain the problem
in your final message instead.

## Evidence: show your work running, don't just describe it

If your change touches anything user-visible in the web UI (a new page, a changed
layout, a fixed rendering bug, a new control), attach a screenshot of the app actually
running with your change before opening the PR:

1. Build and start the app in the background, pointed at a scratch port so it doesn't
   collide with anything else in the container: `cargo run -- &` (or however this
   ticket's change needs it started -- e.g. `UI_PORT=8080 cargo run &`), then wait
   until it's actually accepting connections (poll `curl -sf http://localhost:8080/`
   in a loop for a few seconds rather than a fixed sleep) before screenshotting it.
2. Take a screenshot with headless Chromium (already installed in this image):
   `chromium --headless --disable-gpu --no-sandbox --window-size=1280,800 \
   --screenshot=/tmp/evidence.png http://localhost:8080/<the relevant page>`.
3. Call the `attach_evidence` tool with `image_path` set to that file's path (relative
   to this workspace, e.g. `/tmp/evidence.png` works as-is since it's an absolute
   path) and a short `caption` describing what the screenshot shows. It uploads the
   image to this branch and returns a markdown image snippet -- paste that snippet
   into the PR body you write next (below).
4. Stop the app (`kill %1` or equivalent) before moving on -- don't leave it running
   across turns.

Skip this for changes with nothing user-visible to show (a bugfix in a background
worker, a refactor, a test-only change) -- evidence is for demonstrating a UI change
actually works, not a checkbox to tick on every PR.

## Submitting your work

Once your branch is pushed and you're satisfied with the change, call the
`open_pull_request` tool with a `title` and a `body` describing what changed and why.
If you captured evidence above, include the markdown image snippet `attach_evidence`
gave you somewhere in the body so a reviewer sees it rendered inline. Include a line
`Closes #{{ issue.identifier }}` in the body -- the tracker issue closes automatically
when a human reviews and merges the PR.

Immediately after `open_pull_request` succeeds, call `update_issue_state` with
`state: "in review"`. This does **not** close the issue -- it just tells the harness
your active work here is done for now, so it stops redispatching you to re-report the
same status every few seconds. **Never call `update_issue_state` with `"done"`** --
in this workflow "done" means merged, and that decision belongs to whoever reviews
the PR, not you. If you call `open_pull_request` again later (e.g. after pushing more
work in a retry), it updates the existing PR in place rather than opening a
duplicate -- call `update_issue_state("in review")` again too if the issue had moved
out of it for any reason.

**You are being redispatched to this exact same ticket if you are reading this and the
PR already exists with nothing new to do.** That means `update_issue_state("in
review")` was never actually called (or didn't take effect) the first time. Do **not**
just repeat a status summary and end your turn again -- that changes nothing and you
will be redispatched again in a few seconds, forever. Call `update_issue_state` with
`state: "in review"` right now, this turn, before writing your final message. Only
skip this if there's genuinely new work to do (e.g. reviewer feedback landed).

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
