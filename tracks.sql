PRAGMA journal_mode = WAL;
PRAGMA foreign_keys = ON;

CREATE TABLE tracks (
	id INTEGER PRIMARY KEY ASC,
	uuid TEXT_NOT_NULL,
	genome TEXT NOT NULL,
	platform TEXT NOT NULL,
	dataset TEXT NOT NULL,
	name TEXT NOT NULL,
	regions INTEGER NOT NULL,
	file TEXT NOT NULL,
	UNIQUE(genome, platform, dataset, name));
CREATE INDEX tracks_uuid_idx ON tracks(uuid);