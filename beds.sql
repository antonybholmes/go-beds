PRAGMA journal_mode = WAL;
PRAGMA foreign_keys = ON;

CREATE TABLE beds (
	id INTEGER PRIMARY KEY ASC,
	public_id TEXT_NOT_NULL,
	platform TEXT NOT NULL,
	genome TEXT NOT NULL,
	name TEXT NOT NULL,
	file TEXT NOT NULL,
	UNIQUE(platform, genome, name));
CREATE INDEX beds_public_id_idx ON beds(public_id);