# TODO

The two backlogs below were pending manual browser verification; both are now
automated by the browser e2e suite — `tests/browser.rs` spawns the real axum
server (real store, seeded with 25 image posts through the real
[`ArchiveStore`]; the Bluesky network is never touched), and `e2e-browser/`
drives a real Chromium via Playwright. CI runs it in the `browser-e2e` job of
`.github/workflows/ci.yml`. Locally: `npm install && npx playwright install
chromium` in `e2e-browser/`, then `cargo test --test browser`. The Rust test
skips (passes with a note) when node/Chromium is unavailable.

Formerly-manual checks now asserted by the suite:

- **Responsive layout** (issue #16): no horizontal document scroll at
  360/768/1440px across `/`, `/posts`, `/posts/:id` (reached through the real
  list, so the `at_uri` path encoding is the server's own), `/gallery`,
  `/config` and `/login`; nav disclosure below 768px with all four links +
  Logout revealed; horizontal nav row at 768/1440px; config and dashboard
  health tables reflow into labelled stacked blocks (and render as ordinary
  tables wide); ~44px touch targets on pagination and category filters at
  360px; lightbox open/close at all widths; dark and light color schemes both
  style the page.
- **htmx 4 client-side behavior**: gallery/posts pagination swaps replace
  the grid/list in place (`outerHTML` — fresh nodes, so the card load-in
  animation replays on every page change) without a page reload and push
  canonical URLs; a page switch scrolls the viewport back to the top of the
  gallery; back/forward restore `<main>` from a re-fetched
  full page (reload probe proves the shell never re-executes); the lightbox
  walks across page boundaries forward and back via the same swaps and lands
  on the boundary item with correct URL; back navigation with the lightbox
  open closes it and restores the underlying page; forward through a
  lightbox-pushed history entry re-renders (regression test for handing the
  pushed URL to htmx's own history handling instead of a bare
  `history.pushState`).
- **Real UI with real images**: the suite logs in through the password gate
  (including the wrong-password inline error), and asserts every gallery
  image and posts-list thumbnail decodes (`naturalWidth > 0`) from the real
  `/media/...` routes.

Found by the suite while landing it: config-table values (file paths, DIDs)
are unbreakable strings and forced the document wide on narrow viewports —
fixed with `overflow-wrap: anywhere` on the value cells.

## Remaining

- The browser suite does not yet exercise video media (gallery `<video>`
  thumbs and lightbox video teardown) — covered at the unit and Rust-e2e
  level only.
- Live-updating dashboard panels (e.g. an SSE activity feed built on htmx 4's
  streaming extensions) are an unimplemented idea, not a tracked defect.
