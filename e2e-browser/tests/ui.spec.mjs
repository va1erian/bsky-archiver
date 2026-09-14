// Browser e2e suite for the bsky-archiver web UI. Runs against the real
// axum server built by `tests/browser.rs` (real store seeded with 25 media
// posts = 3 gallery pages at page_size=10, real htmx, real images served
// from the /media/ routes).
//
// Covers the two behavior groups recorded in TODO.md before this suite
// existed: the htmx 4 client-side behaviors (pagination swaps without a
// page reload, fetch-based back/forward history restore, lightbox page walk)
// and the
// responsive viewport checks (layout, nav disclosure, table reflow, touch
// targets, dark/light) from the responsive-layout work.
import { test, expect } from "@playwright/test";

const UI_PASSWORD = process.env.UI_PASSWORD ?? "e2e-ui-password";
const GALLERY = "/gallery?page_size=10";

// Sets a window marker that any full page reload would wipe. The htmx
// fragment/history swaps below must keep it, proving no navigation happened.
async function plantReloadProbe(page, scope = "#gallery-grid") {
  await page.evaluate((sel) => {
    window.__reloadProbe = 42;
    window.__gridNode = document.querySelector(sel);
  }, scope);
}

async function reloadProbe(page) {
  return page.evaluate(() => window.__reloadProbe ?? null);
}

async function gridIsSameNode(page) {
  return page.evaluate(
    () => document.querySelector("#gallery-grid") === window.__gridNode,
  );
}

async function login(page) {
  await page.goto("/");
  await page.waitForURL("**/login");
  await page.fill("#password", UI_PASSWORD);
  await page.click("button[type=submit]");
  await page.waitForURL("/");
}

async function everyImageLoaded(page, scope = "") {
  return page.evaluate((sel) => {
    const images = document.querySelectorAll(sel);
    return Array.from(images).map((img) => img.complete && img.naturalWidth > 0);
  }, scope);
}

// ---------------------------------------------------------------------
// Real UI with real images
// ---------------------------------------------------------------------

test("gallery renders three real pages of loadable images", async ({ page }) => {
  await login(page);
  await page.goto(GALLERY);
  await expect(page.locator(".pagination .item-meta")).toContainText(
    "Page 1 of 3 (25 total)",
  );
  await expect(page.locator(".gallery-grid .item-card")).toHaveCount(10);
  await page.waitForLoadState("networkidle");
  const states = await everyImageLoaded(page, ".gallery-grid img");
  expect(states.length).toBe(10);
  for (const [i, ok] of states.entries()) {
    expect(ok, `gallery image ${i} must decode`).toBe(true);
  }
  // The account viewer moved to its own page: the gallery links there instead
  // of embedding the form itself.
  await expect(page.locator("#gallery-account-actor")).toHaveCount(0);
  await expect(page.locator('nav.main-nav a[href="/browser"]')).toBeVisible();
});

test("posts list renders cards with loadable thumbnails", async ({ page }) => {
  await login(page);
  await page.goto("/posts?page_size=10");
  await expect(page.locator(".pagination .item-meta")).toContainText(
    "Page 1 of 3 (25 total)",
  );
  await page.waitForLoadState("networkidle");
  const states = await everyImageLoaded(page, ".card-grid img.thumb");
  expect(states.length).toBe(10);
  for (const [i, ok] of states.entries()) {
    expect(ok, `posts thumbnail ${i} must decode`).toBe(true);
  }
  // Post detail rounds-trips from the real "View post" link.
  await page.locator(".card-grid .item-card a", { hasText: "View post" }).first().click();
  await expect(page.locator("main")).toContainText("an e2e browser");
});

// ---------------------------------------------------------------------
// htmx 4 client-side behavior
// ---------------------------------------------------------------------

test("pagination swaps replace the grid without a page reload and replay the card animation", async ({
  page,
}) => {
  await login(page);
  await page.goto(GALLERY);
  await plantReloadProbe(page);

  await page.locator(".pagination a[rel=next]").click();
  await expect(page.locator(".pagination .item-meta")).toContainText("Page 2 of 3");
  await expect(page).toHaveURL(/page=2/);
  expect(await reloadProbe(page)).toBe(42); // no full page reload
  // outerHTML replaces the grid wholesale; the fresh nodes are what replays
  // the card-in load-in animation on every page change (morphing kept the
  // old nodes and silently dropped the animation).
  expect(await gridIsSameNode(page)).toBe(false);
  expect(
    await page.evaluate(
      () =>
        getComputedStyle(document.querySelector(".gallery-grid .gallery-item"))
          .animationName,
    ),
  ).toBe("card-in");

  // A page switch puts the viewport back at the top of the gallery: scroll
  // down to the pagination row, click Next, and the page is back at the top.
  await page.evaluate(() => window.scrollTo(0, document.body.scrollHeight));
  await page.locator(".pagination a[rel=next]").click();
  await expect(page.locator(".pagination .item-meta")).toContainText("Page 3 of 3");
  expect(await page.evaluate(() => window.scrollY)).toBe(0);

  // The pagination links live inside the swapped grid: clicking First
  // returns to page 1 over the same target.
  await page.locator(".pagination a[rel=first]").click();
  await expect(page.locator(".pagination .item-meta")).toContainText("Page 1 of 3");
  await expect(page).toHaveURL(/page=1/);
  expect(await reloadProbe(page)).toBe(42);
});

test("back/forward navigation restores main via htmx's fetch restore", async ({
  page,
}) => {
  await login(page);
  await page.goto(GALLERY);
  await plantReloadProbe(page);

  await page.locator(".pagination a[rel=next]").click();
  await expect(page.locator(".pagination .item-meta")).toContainText("Page 2 of 3");

  await page.goBack();
  await expect(page.locator(".pagination .item-meta")).toContainText("Page 1 of 3");
  // The restored entry is the original page-1 URL prior to the htmx push
  // (canonical pagination hrefs live on the links, not the initial load).
  await expect(page).not.toHaveURL(/page=2/);
  // The restore swapped <main> out of a re-fetched full page; the surrounding
  // shell (nav links, scripts) was never re-executed, so the probe survives.
  expect(await reloadProbe(page)).toBe(42);
  // The header is not part of the swap: the nav row is still there.
  await expect(page.locator('nav.main-nav a[href="/gallery"]')).toBeVisible();

  await page.goForward();
  await expect(page).toHaveURL(/page=2/);
  await expect(page.locator(".pagination .item-meta")).toContainText("Page 2 of 3");
  expect(await reloadProbe(page)).toBe(42);
});

test("lightbox walks across the page boundary forward and back", async ({
  page,
}) => {
  await login(page);
  await page.goto(GALLERY);
  await page.locator(".gallery-grid .gallery-item").first().click();
  await expect(page.locator("#lightbox-counter")).toHaveText("1 / 10");

  // Ten ArrowRights: nine walk within page 1, the tenth loads page 2.
  for (let i = 2; i <= 10; i++) {
    await page.keyboard.press("ArrowRight");
    await expect(page.locator("#lightbox-counter")).toHaveText(`${i} / 10`);
  }
  await page.keyboard.press("ArrowRight"); // boundary -> adjacent page load
  await expect(page).toHaveURL(/page=2/);
  await expect(page.locator("#lightbox-counter")).toHaveText("1 / 10");
  await expect(page.locator(".pagination .item-meta")).toContainText("Page 2 of 3");

  // Close: the lightbox-driven swap leaves the browser on page 2.
  await page.click("#lightbox-close");
  await expect(page.locator("#lightbox")).not.toHaveAttribute("open");
  await expect(page.locator(".pagination .item-meta")).toContainText("Page 2 of 3");

  // Walk back across the boundary: lands on page 1's last item.
  await page.locator(".gallery-grid .gallery-item").first().click();
  await expect(page.locator("#lightbox-counter")).toHaveText("1 / 10");
  await page.keyboard.press("ArrowLeft"); // boundary -> previous page load
  await expect(page).toHaveURL(/page=1/);
  await expect(page.locator("#lightbox-counter")).toHaveText("10 / 10");
  await expect(page.locator(".pagination .item-meta")).toContainText("Page 1 of 3");
});

test("back navigation with the lightbox open closes it and restores the page", async ({
  page,
}) => {
  await login(page);
  await page.goto(GALLERY);
  await page.locator(".pagination a[rel=next]").click();
  await expect(page.locator(".pagination .item-meta")).toContainText("Page 2 of 3");
  await plantReloadProbe(page);

  await page.locator(".gallery-grid .gallery-item").first().click();
  await expect(page.locator("#lightbox-counter")).toHaveText("1 / 10");

  await page.goBack();
  await expect(page.locator(".pagination .item-meta")).toContainText("Page 1 of 3");
  await expect(page).not.toHaveURL(/page=2/);
  expect(await reloadProbe(page)).toBe(42);
  expect(
    await page.evaluate(() => document.getElementById("lightbox").open),
  ).toBe(false);
});

test("forward through a lightbox-pushed history entry re-renders", async ({
  page,
}) => {
  // Regression test for the bare history.pushState mirror: htmx ignores
  // popstate entries without its {htmx: true} state marker, so only its own
  // push (via the ajax `push` option) makes forward re-render.
  await login(page);
  await page.goto(GALLERY);
  await page.locator(".gallery-grid .gallery-item").first().click();
  await expect(page.locator("#lightbox-counter")).toHaveText("1 / 10");

  for (let i = 0; i < 10; i++) {
    await page.keyboard.press("ArrowRight");
  }
  await expect(page).toHaveURL(/page=2/);
  await expect(page.locator(".pagination .item-meta")).toContainText("Page 2 of 3");
  await page.click("#lightbox-close");

  await page.goBack();
  await expect(page.locator(".pagination .item-meta")).toContainText("Page 1 of 3");
  await page.goForward();
  await expect(page).toHaveURL(/page=2/);
  await expect(page.locator(".pagination .item-meta")).toContainText("Page 2 of 3");
});

test("posts list pagination works the same way", async ({ page }) => {
  await login(page);
  await page.goto("/posts?page_size=10");
  await plantReloadProbe(page, "#posts-list");
  await page.locator(".pagination a[rel=next]").click();
  await expect(page.locator(".pagination .item-meta")).toContainText("Page 2 of 3");
  expect(await reloadProbe(page)).toBe(42);
  await page.goBack();
  await expect(page.locator(".pagination .item-meta")).toContainText("Page 1 of 3");
  expect(await reloadProbe(page)).toBe(42);
});

// ---------------------------------------------------------------------
// Browser page (account viewer + saved favorites)
// ---------------------------------------------------------------------

test("browser page shows its handle form and its saved-accounts panel", async ({
  page,
}) => {
  await login(page);
  await page.goto("/browser");
  await expect(page.locator("main h1")).toHaveText("Browser");
  await expect(page.locator("#account-actor")).toBeVisible();
  await expect(page.locator("#saved-accounts-panel")).toBeVisible();
  await expect(page.locator("#saved-accounts-panel")).toContainText(
    "No saved accounts yet",
  );

  // The old /gallery/account URL lands on the browser page.
  await page.goto("/gallery/account?actor=bob.bsky.social");
  await expect(page).toHaveURL(/\/browser\?actor=/);
  await expect(page.locator("main h1")).toHaveText("Browser");
  // No network in the e2e environment, so the browse fails inline.
  await expect(page.locator("[role=alert]").last()).toContainText(
    "could not load",
  );
});

test("saving and removing a favorite account through the UI round-trips", async ({
  page,
}) => {
  await login(page);
  await page.goto("/browser");

  // Save via the panel form (htmx swap; no page reload).
  await plantReloadProbe(page, "#saved-accounts-panel");
  await page.fill("#saved-account-handle", "bob.bsky.social");
  await page.click("#saved-accounts-panel form.source-add-form button");
  const panel = page.locator("#saved-accounts-panel");
  await expect(panel).toContainText("bob.bsky.social");
  expect(await reloadProbe(page)).toBe(42);

  // Re-save (No-JS path is the same form; a duplicate stays one row).
  await page.fill("#saved-account-handle", "bob.bsky.social");
  await page.click("#saved-accounts-panel form.source-add-form button");
  await expect(panel.locator("td", { hasText: "bob.bsky.social" })).toHaveCount(1);

  // Remove: the panel empties again.
  await page.locator(
    "#saved-accounts-panel td form[action*='/browser/saved/'] button",
  ).first().click();
  await expect(panel).toContainText("No saved accounts yet");
});

// ---------------------------------------------------------------------
// Login gate
// ---------------------------------------------------------------------

test("login rejects a wrong password with an inline error", async ({ page }) => {
  await page.goto("/login");
  await page.fill("#password", "not-the-password");
  await page.click("button[type=submit]");
  await expect(page.locator("[role=alert]")).toContainText("Incorrect password");
});

test("login page renders without horizontal scroll at 360px", async ({ page }) => {
  await page.setViewportSize({ width: 360, height: 800 });
  await page.goto("/login");
  const scroll = await page.evaluate(() => ({
    scrollWidth: document.scrollingElement.scrollWidth,
    innerWidth: window.innerWidth,
  }));
  expect(scroll.scrollWidth).toBeLessThanOrEqual(scroll.innerWidth + 1);
});

// ---------------------------------------------------------------------
// Responsive layout (360 / 768 / 1440)
// ---------------------------------------------------------------------

for (const [width, height] of [
  [360, 800],
  [768, 900],
  [1440, 900],
]) {
  test(`responsive layout at ${width}px`, async ({ page }) => {
    await login(page);
    await page.setViewportSize({ width, height });

    // Post detail: reached through the real list so the URL uses the
    // server's own percent-encoding of the at_uri path segment.
    await page.goto("/posts?page_size=10");
    const detailHref = await page
      .locator(".card-grid .item-card a", { hasText: "View post" })
      .first()
      .getAttribute("href");
    const urls = ["/", detailHref, "/posts?page_size=10", GALLERY, "/browser", "/config"];

    for (const path of urls) {
      await page.goto(path);
      await expect(page.locator("main")).toBeVisible();

      const configTableOnly = path.startsWith("/config") || path.startsWith("/browser");
      let offenders = [];
      const scroll = await page.evaluate(() => ({
        scrollWidth: document.scrollingElement.scrollWidth,
        innerWidth: window.innerWidth,
      }));
      if (configTableOnly) {
        offenders = await page.evaluate(() => {
          const vw = window.innerWidth;
          return Array.from(document.querySelectorAll("*"))
            .filter((el) => {
              const r = el.getBoundingClientRect();
              return r.right > vw + 1 && r.width > 0;
            })
            .slice(0, 12)
            .map((el) => ({
              tag: el.tagName,
              cls: String(el.className ?? ""),
              right: Math.round(el.getBoundingClientRect().right),
              w: Math.round(el.getBoundingClientRect().width),
              text: (el.textContent ?? "").trim().slice(0, 50),
            }));
        });
      }
      expect(
        scroll.scrollWidth,
        `no horizontal document scroll at ${width}px on ${path} — offenders: ${JSON.stringify(offenders)}`,
      ).toBeLessThanOrEqual(scroll.innerWidth + 1);

      // Nav: disclosure behind <details> below 768px, horizontal row above.
      const summary = page.locator(".nav-menu > summary");
      const galleryLink = page.locator('nav.main-nav a[href="/gallery"]');
      if (width < 768) {
        await expect(summary).toBeVisible();
        await expect(galleryLink).toBeHidden();
        await summary.click();
        await expect(galleryLink).toBeVisible();
        await expect(page.locator('nav.main-nav a[href="/browser"]')).toBeVisible();
        await expect(page.locator('nav.main-nav a[href="/posts"]')).toBeVisible();
        await expect(page.locator('nav.main-nav a[href="/config"]')).toBeVisible();
      } else {
        await expect(summary).toBeHidden();
        await expect(galleryLink).toBeVisible();
        await expect(page.locator('nav.main-nav a[href="/browser"]')).toBeVisible();
      }

      // Touch targets on the paginated paths at phone width: the CSS media
      // query enforces the ~44px minimum only below 768px.
      if (width < 768 && (path.startsWith("/gallery") || path.startsWith("/posts"))) {
        const pagination = page.locator(".pagination a").first();
        if ((await pagination.count()) > 0) {
          const box = await pagination.boundingBox();
          expect(box.height, `pagination touch target at ${width}px`).toBeGreaterThanOrEqual(
            44,
          );
          const filter = page.locator(".category-filter a").first();
          if ((await filter.count()) > 0) {
            const fbox = await filter.boundingBox();
            expect(
              fbox.height,
              `category-filter touch target at ${width}px`,
            ).toBeGreaterThanOrEqual(44);
          }
        }
      }

      // Table reflow: data tables become labelled stacked blocks below 768px.
      const table = page.locator(".health-table, .config-table").first();
      if ((await table.count()) > 0) {
        const cell = table.locator("td").first();
        const reflow = await cell.evaluate((td) => ({
          tdDisplay: getComputedStyle(td).display,
          label: getComputedStyle(td, "::before").content,
          headDisplay: getComputedStyle(td.closest("table").querySelector("thead")).display,
        }));
        if (width < 768) {
          expect(reflow.headDisplay, "thead hidden at phone width").toBe("none");
          expect(reflow.tdDisplay).toBe("block");
          expect(reflow.label).not.toBe("none");
        } else {
          expect(reflow.headDisplay).not.toBe("none");
          expect(reflow.label).toBe("none");
        }
      }
    }

    // Lightbox opens and closes at every width.
    await page.goto(GALLERY);
    await page.locator(".gallery-grid .gallery-item").first().click();
    await expect(page.locator("#lightbox-counter")).toHaveText("1 / 10");
    await page.click("#lightbox-close");
    expect(
      await page.evaluate(() => document.getElementById("lightbox").open),
    ).toBe(false);
  });
}

test("dark and light color schemes both style the page", async ({ page }) => {
  await login(page);
  await page.emulateMedia({ colorScheme: "light" });
  await page.goto("/");
  const light = await page.evaluate(
    () => getComputedStyle(document.documentElement).color,
  );
  await page.emulateMedia({ colorScheme: "dark" });
  const dark = await page.evaluate(
    () => getComputedStyle(document.documentElement).color,
  );
  expect(light, "Pico must not be bypassed: text color follows the scheme").not.toBe(
    dark,
  );
});
