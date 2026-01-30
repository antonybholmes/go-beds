PRAGMA journal_mode = WAL;
PRAGMA foreign_keys = ON;

CREATE TABLE sample (
	id TEXT NOT NULL,
	genome TEXT NOT NULL,
	assembly TEXT NOT NULL,
	platform TEXT NOT NULL,
	name TEXT NOT NULL,
	description TEXT NOT NULL DEFAULT '',
	type TEXT NOT NULL DEFAULT 'BED',
	regions INTEGER NOT NULL DEFAULT 0,
	tags TEXT NOT NULL DEFAULT '');

CREATE TABLE chromosomes (
	id INTEGER PRIMARY KEY,
	name TEXT NOT NULL UNIQUE);

CREATE TABLE regions (
	id INTEGER PRIMARY KEY,
	chr_id INTEGER NOT NULL,
	start INTEGER NOT NULL,
	end INTEGER NOT NULL,
	score REAL NOT NULL DEFAULT 0,
	name TEXT NOT NULL DEFAULT '',
	tags TEXT NOT NULL DEFAULT '',
	UNIQUE(chr_id, start, end),
	FOREIGN KEY (chr_id) REFERENCES chromosomes(id) ON DELETE CASCADE);
CREATE INDEX regions_name_idx ON regions(name);
CREATE INDEX regions_tags_idx ON regions(tags);