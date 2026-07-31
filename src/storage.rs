//! On-disk archive storage and the SQLite query index built on top of it.
//! The JSON files and media on disk are the source of truth; the SQLite
//! index is a rebuildable query layer over them.
