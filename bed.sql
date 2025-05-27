PRAGMA journal_mode = WAL;
PRAGMA foreign_keys = ON;

CREATE TABLE track (
	id INTEGER PRIMARY KEY ASC,
	public_id TEXT_NOT_NULL,
	genome TEXT NOT NULL,
	platform TEXT NOT NULL,
	name TEXT NOT NULL,
	description TEXT NOT NULL DEFAULT '',
	track_type TEXT NOT NULL,
	regions INTEGER NOT NULL DEFAULT -1,
	tags TEXT NOT NULL DEFAULT '');

CREATE TABLE regions (
	id INTEGER PRIMARY KEY ASC,
	chr TEXT NOT NULL,
	start INTEGER NOT NULL,
	end INTEGER NOT NULL,
	score REAL NOT NULL DEFAULT 0,
	name TEXT NOT NULL DEFAULT '',
	tags TEXT NOT NULL DEFAULT '',
	UNIQUE(chr, start, end));
CREATE INDEX regions_name_idx ON regions(name);
CREATE INDEX regions_tags_idx ON regions(tags);