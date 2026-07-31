//! Periodic REST polling for likes and bookmarks (not available on the
//! firehose), and the REST-polling fallback path for authored posts when the
//! firehose connection is unavailable. Uses adaptive intervals with
//! exponential backoff and jitter.
