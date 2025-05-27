PRAGMA journal_mode = WAL;
PRAGMA foreign_keys = ON;

CREATE TABLE tracks (
	id INTEGER PRIMARY KEY ASC,
	public_id TEXT_NOT_NULL,
	genome TEXT NOT NULL,
	platform TEXT NOT NULL,
	dataset TEXT NOT NULL,
	name TEXT NOT NULL,
	track_type TEXT NOT NULL,
	regions INTEGER NOT NULL DEFAULT 0,
	url TEXT NOT NULL,
	tags TEXT NOT NULL,
	UNIQUE(genome, platform, dataset, name));
CREATE INDEX tracks_public_id_idx ON tracks(public_id);