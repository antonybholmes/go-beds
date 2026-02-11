# -*- coding: utf-8 -*-
"""
Encode read counts per base in 2 bytes

@author: Antony Holmes
"""
import argparse
import os
import sqlite3
import sys

import pandas as pd
import uuid_utils as uuid
from nanoid import generate

DIR = "../data/modules/beds"

parser = argparse.ArgumentParser()

parser.add_argument("-b", "--bed", help="bed file")
parser.add_argument("-p", "--platform", default="ChIP-seq", help="data plaform")

parser.add_argument(
    "--samples",
    default="samples.tsv",
    help="tsv file with columns: dataset, sample, paired, bam, genome, assembly, type",
)

parser.add_argument("-o", "--out", default=DIR, help="output directory")
args = parser.parse_args()

samples_file = args.samples
bed = args.bed  # sys.argv[2]
platform = args.platform
genome = args.genome  # sys.argv[3]
assembly = args.assembly  # sys.argv[4]

out = args.out

HUMAN_CHRS = [
    "chr1",
    "chr2",
    "chr3",
    "chr4",
    "chr5",
    "chr6",
    "chr7",
    "chr8",
    "chr9",
    "chr10",
    "chr11",
    "chr12",
    "chr13",
    "chr14",
    "chr15",
    "chr16",
    "chr17",
    "chr18",
    "chr19",
    "chr20",
    "chr21",
    "chr22",
    "chrX",
    "chrY",
    "chrM",
]

CHR_MAP = {"Human": {chr: idx + 1 for idx, chr in enumerate(HUMAN_CHRS)}}

MOUSE_CHRS = [
    "chr1",
    "chr2",
    "chr3",
    "chr4",
    "chr5",
    "chr6",
    "chr7",
    "chr8",
    "chr9",
    "chr10",
    "chr11",
    "chr12",
    "chr13",
    "chr14",
    "chr15",
    "chr16",
    "chr17",
    "chr18",
    "chr19",
    "chrX",
    "chrY",
    "chrM",
]

CHR_MAP["Mouse"] = {chr: idx + 1 for idx, chr in enumerate(MOUSE_CHRS)}

db = os.path.join(
    dir,
    "beds.db",
)

if os.path.exists(db):
    os.remove(db)

conn = sqlite3.connect(db)
conn.row_factory = sqlite3.Row
cursor = conn.cursor()

cursor.execute("PRAGMA journal_mode = WAL;")
cursor.execute("PRAGMA foreign_keys = ON;")

cursor.execute(
    f"""
    CREATE TABLE genomes (
        id INTEGER PRIMARY KEY,
        public_id TEXT NOT NULL UNIQUE,
        name TEXT NOT NULL,
        scientific_name TEXT NOT NULL,
        UNIQUE(name, scientific_name));
    """,
)

cursor.execute(
    f"INSERT INTO genomes (id, public_id, name, scientific_name) VALUES (1, '{uuid.uuid7()}', 'Human', 'Homo sapiens');"
)
cursor.execute(
    f"INSERT INTO genomes (id, public_id, name, scientific_name) VALUES (2, '{uuid.uuid7()}', 'Mouse', 'Mus musculus');"
)

cursor.execute(
    f"""
    CREATE TABLE assemblies (
        id INTEGER PRIMARY KEY,
        public_id TEXT NOT NULL UNIQUE,
        genome_id INTEGER NOT NULL,
        name TEXT NOT NULL UNIQUE,
        FOREIGN KEY (genome_id) REFERENCES genomes(id) ON DELETE CASCADE);
    """,
)

cursor.execute(
    f"INSERT INTO assemblies (id, public_id, genome_id, name) VALUES (1, '{uuid.uuid7()}', 1, 'hg19');"
)
cursor.execute(
    f"INSERT INTO assemblies (id, public_id, genome_id, name) VALUES (2, '{uuid.uuid7()}', 1, 'GRCh38');"
)
cursor.execute(
    f"INSERT INTO assemblies (id, public_id, genome_id, name) VALUES (3, '{uuid.uuid7()}', 2, 'GRCm39');"
)

cursor.execute(
    f"""
    CREATE TABLE chromosomes (
        id INTEGER PRIMARY KEY,
        public_id TEXT NOT NULL UNIQUE,
        genome_id INTEGER NOT NULL,
        chr_id INTEGER NOT NULL,
        name TEXT NOT NULL,
        FOREIGN KEY (genome_id) REFERENCES genomes(id) ON DELETE CASCADE);
    """,
)


for chr in HUMAN_CHRS:
    cursor.execute(
        f"INSERT INTO chromosomes (public_id, genome_id, chr_id, name) VALUES ('{str(uuid.uuid7())}', 1, {CHR_MAP['Human'][chr]}, '{chr}');",
    )

for chr in MOUSE_CHRS:
    cursor.execute(
        f"INSERT INTO chromosomes (public_id, genome_id, chr_id, name) VALUES ('{str(uuid.uuid7())}', 2, {CHR_MAP['Mouse'][chr]}, '{chr}');",
    )


cursor.execute(
    f""" CREATE TABLE permissions (
	id INTEGER PRIMARY KEY ASC,
    public_id TEXT NOT NULL UNIQUE,
	name TEXT NOT NULL);
"""
)

cursor.execute(
    f"""CREATE TABLE dataset_permissions (
	dataset_id INTEGER,
    permission_id INTEGER,
    PRIMARY KEY(dataset_id, permission_id),
    FOREIGN KEY (dataset_id) REFERENCES datasets(id) ON DELETE CASCADE,
    FOREIGN KEY (permission_id) REFERENCES permissions(id) ON DELETE CASCADE);
"""
)

rdfViewId = str(uuid.uuid7())

cursor.execute(
    f"INSERT INTO permissions (id, public_id, name) VALUES (1, '{rdfViewId}', 'rdf:view');"
)

cursor.execute(
    f""" CREATE TABLE datasets (
	id INTEGER PRIMARY KEY,
    public_id TEXT NOT NULL UNIQUE,
	assembly_id INTEGER NOT NULL,
    technology_id INTEGER NOT NULL,
    name TEXT NOT NULL, 
    description TEXT NOT NULL DEFAULT '',
    tags TEXT NOT NULL DEFAULT '',
	FOREIGN KEY(assembly_id) REFERENCES assemblies(id) ON DELETE CASCADE,
    FOREIGN KEY(technology_id) REFERENCES technologies(id) ON DELETE CASCADE
);
"""
)

cursor.execute(
    f""" CREATE TABLE sample_types (
	id INTEGER PRIMARY KEY ASC,
    public_id TEXT NOT NULL UNIQUE,
	name TEXT NOT NULL);
"""
)

cursor.execute(
    f"INSERT INTO sample_types (id, public_id, name) VALUES (1, '{uuid.uuid7()}', 'BED');"
)
cursor.execute(
    f"INSERT INTO sample_types (id, public_id, name) VALUES (2, '{uuid.uuid7()}', 'Remote BigBed');"
)

cursor.execute(
    f""" CREATE TABLE samples (
	id INTEGER PRIMARY KEY,
    public_id TEXT NOT NULL UNIQUE,
	dataset_id INTEGER NOT NULL,
	name TEXT NOT NULL UNIQUE,
    type_id INTEGER NOT NULL,
    url TEXT NOT NULL DEFAULT '',
    description TEXT NOT NULL DEFAULT '',
    tags TEXT NOT NULL DEFAULT '',
	FOREIGN KEY(dataset_id) REFERENCES datasets(id) ON DELETE CASCADE,
    FOREIGN KEY(type_id) REFERENCES sample_types(id) ON DELETE CASCADE
);"""
)

cursor.execute(
    f"""
    CREATE TABLE regions (
        id INTEGER PRIMARY KEY,
        public_id TEXT NOT NULL UNIQUE,
        sample_id INTEGER NOT NULL,
        chr_id INTEGER NOT NULL,
        start INTEGER NOT NULL,
        end INTEGER NOT NULL,
        score REAL,
        FOREIGN KEY (chr_id) REFERENCES chromosomes(id) ON DELETE CASCADE,
        FOREIGN KEY (sample_id) REFERENCES samples(id) ON DELETE CASCADE);
    """,
)

df_samples = pd.read_csv(samples_file, sep="\t", header=0, keep_default_na=False)

df_seq_samples = df_samples[df_samples["type"] == "Seq"]
df_remote_bigwig_samples = df_samples[df_samples["type"] == "Remote BigWig"]

for i, row in df_seq_samples.iterrows():
    dataset = row["dataset"]
    sample = row["sample"]
    paired = row["paired"] == "True"
    bed = row["file"]
    genome = row["genome"]
    assembly = row["assembly"]
    technology = row["technology"]

    with open(bed, "r") as fin:
        for line in fin:
            line = line.strip()

            # skip bed headers
            if line.startswith("track"):
                continue

            tokens = line.split("\t")
            chr = tokens[0]

            if chr not in chr_map:
                continue

            chr_id = chr_map[chr]
            start = tokens[1]
            end = tokens[2]
            score = tokens[3] if len(tokens) > 3 else ""

            if score != "":
                print(
                    f"INSERT INTO regions (chr_id, start, end, score) VALUES ({chr_id}, {start}, {end}, {score});",
                    file=f,
                )
            else:
                print(
                    f"INSERT INTO regions (chr_id, start, end) VALUES ({chr_id}, {start}, {end});",
                    file=f,
                )

            c += 1

        print("COMMIT;", file=f)

        print("BEGIN TRANSACTION;", file=f)
        print(
            f"INSERT INTO sample (id, genome, assembly, platform, name, type, regions) VALUES ('{public_id}', '{genome}', '{assembly}', '{platform}', '{sample}', 'BED', {c});",
            file=f,
        )
        print("COMMIT;", file=f)
