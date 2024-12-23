PRAGMA journal_mode = WAL;
PRAGMA foreign_keys = ON;

CREATE TABLE info (
	id INTEGER PRIMARY KEY ASC,
	public_id TEXT_NOT_NULL,
	platform TEXT NOT NULL,
	genome TEXT NOT NULL,
	name TEXT NOT NULL);

CREATE TABLE bed (
	id INTEGER PRIMARY KEY ASC,
	chr TEXT NOT NULL,
	start INTEGER NOT NULL,
	end INTEGER NOT NULL,
	score REAL,
	name TEXT,
	tags TEXT,
	UNIQUE(chr, start, end));
CREATE INDEX bed_name_idx ON bed(name);
CREATE INDEX bed_tags_idx ON bed(tags);