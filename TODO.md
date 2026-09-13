# TODO

## Browser-based responsive viewport tests

The responsive layout work (issue #16) was verified manually in a browser at
360px, 768px, and 1440px viewport widths across every page (`/login`, `/`
dashboard, `/posts`, `/posts/:id`, `/gallery`, `/config`). The existing
`tests/e2e.rs` drives the axum router directly and asserts routing/markup
presence — it cannot observe layout, so none of the following can be checked
there.

Once a browser/computer-use e2e suite lands, add automated coverage that
asserts these layouts at fixed viewport widths, replacing the manual
verification:

- **No horizontal document scroll at 360px** on any page —
  `document.scrollingElement.scrollWidth` must not exceed the viewport width
  (individual elements such as the `post_detail` raw-JSON block may scroll
  internally).
- **Nav disclosure at 360px** — the header nav is collapsed behind the
  `<details>`/`<summary>` toggle; opening it reveals all four links plus Logout,
  each independently tappable, and Logout still submits `POST /logout`.
- **Nav row at 768px / 1440px** — the nav renders as the horizontal row,
  visually unchanged from the desktop baseline.
- **Table reflow at 360px** — the config and dashboard health tables render as
  stacked, labelled key-value blocks; a long Detail error string wraps instead
  of widening the page. At 768px / 1440px both render as ordinary tables.
- **Touch targets at 360px** — pagination controls and the gallery/posts
  category-filter links are at least 44px in their smallest dimension with
  visible separation between adjacent targets.
- **Lightbox** — opens, closes on backdrop click, and closes via the ✕ button at
  all three widths.
- **Dark and light mode** — both render correctly (Pico's
  `prefers-color-scheme` handling must not be bypassed).

## htmx 4 client-side verification

The vendored htmx was upgraded from 1.9.12 to 4.0.0. Server-side behavior is
covered by `cargo test` (fragment vs. full-page branching, history-restore
carve-out), but the client-side behaviors below need a real browser; once a
browser e2e suite lands, automate them alongside the viewport tests above:

- **Back/forward after paginating** — on `/gallery` and `/posts`, paginate a
  few pages, then use the browser back/forward buttons: htmx 4 re-fetches the
  URL and swaps `<main>` (`[hx-history-elt]`) out of the full page, so the
  grid/list is restored, the header, accent picker and lightbox script do not
  re-run, and the URL bar always matches the rendered page.
- **Lightbox page walk** — arrow/swipe past the last item of a page loads the
  adjacent page via `outerMorph`, pushes its URL, and lands on the boundary
  item; closing the lightbox afterwards leaves a consistent grid.
- **Lightbox auto-close on restore** — with the lightbox open, hitting back
  closes it (the restore swap fires `htmx:after:swap` on `<body>`).
- **Forward through lightbox-pushed entries** — a known pre-existing quirk:
  `loadAdjacentPage` mirrors `hx-push-url` with a bare
  `history.pushState(null, …)`, so forward through such an entry does not
  re-render (htmx ignores history entries without its `{htmx: true}` state
  marker). Passing `push: "true"` to `htmx.ajax()` instead of the manual
  `pushState` would hand the entry to htmx's history handling.
