// Playwright config for the browser e2e suite. The test process does not
// start the application under test: `tests/browser.rs` builds the real axum
// router (real store, real templates, real static assets) on an ephemeral
// port and passes the base URL and UI password in via environment.
import { defineConfig } from "@playwright/test";

const baseUrl = process.env.BASE_URL ?? "http://127.0.0.1:8080";
// Only meaningful when a server was never passed in; tests assume a server
// they did not start, so there is no webServer config here on purpose.

export default defineConfig({
  testDir: "./tests",
  workers: 1,
  retries: process.env.CI ? 1 : 0,
  timeout: 120_000,
  globalTimeout: 600_000,
  expect: { timeout: 10_000 },
  reporter: process.env.CI ? [["list"], ["html", { open: "never" }]] : [["list"]],
  use: {
    baseURL: baseUrl,
    trace: "retain-on-failure",
    screenshot: "only-on-failure",
  },
});
