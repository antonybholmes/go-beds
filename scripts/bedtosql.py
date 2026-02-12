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

CHR_MAP = {
    "Human": {chr: idx + 1 for idx, chr in enumerate(HUMAN_CHRS)},
    "Mouse": {chr: idx + 1 for idx, chr in enumerate(MOUSE_CHRS)},
}

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

assembly_map = {"hg19": 1, "GRCh38": 2, "GRCm39": 3}


cursor.execute(
    f"""
    CREATE TABLE technologies (
        id INTEGER PRIMARY KEY,
        public_id TEXT NOT NULL UNIQUE,
        name TEXT NOT NULL UNIQUE);
    """,
)

cursor.execute(
    f"INSERT INTO technologies (id, public_id, name) VALUES (1, '{uuid.uuid7()}', 'ChIP-seq');"
)
cursor.execute(
    f"INSERT INTO technologies (id, public_id, name) VALUES (2, '{uuid.uuid7()}', 'RNA-seq');"
)
cursor.execute(
    f"INSERT INTO technologies (id, public_id, name) VALUES (3, '{uuid.uuid7()}', 'CUT&RUN');"
)

technology_map = {"ChIP-seq": 1, "RNA-seq": 2, "CUT&RUN": 3}

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
    technology_id INTEGER NOT NULL,
	name TEXT NOT NULL UNIQUE,
    type_id INTEGER NOT NULL,
    description TEXT NOT NULL DEFAULT '',
    tags TEXT NOT NULL DEFAULT '',
	FOREIGN KEY(dataset_id) REFERENCES datasets(id) ON DELETE CASCADE,
    FOREIGN KEY(technology_id) REFERENCES technologies(id) ON DELETE CASCADE,
    FOREIGN KEY(type_id) REFERENCES sample_types(id) ON DELETE CASCADE
);"""
)

cursor.execute(
    f"""
    CREATE TABLE regions (
        id INTEGER PRIMARY KEY,
        sample_id INTEGER NOT NULL,
        chr_id INTEGER NOT NULL,
        start INTEGER NOT NULL,
        end INTEGER NOT NULL,
        score REAL NOT NULL 0,
        FOREIGN KEY (chr_id) REFERENCES chromosomes(id) ON DELETE CASCADE,
        FOREIGN KEY (sample_id) REFERENCES samples(id) ON DELETE CASCADE);
    """,
)

df_samples = pd.read_csv(samples_file, sep="\t", header=0, keep_default_na=False)

df_seq_samples = df_samples[df_samples["type"] == "Seq"]
df_remote_bigwig_samples = df_samples[df_samples["type"] == "Remote BigWig"]

dataset_map = {}
sample_index = 1
for i, row in df_seq_samples.iterrows():
    dataset = row["dataset"]
    sample = row["sample"]
    paired = row["paired"] == "True"
    bed = row["file"]
    assembly = row["assembly"]
    technology = row["technology"]

    if dataset not in dataset_map:
        dataset_id = len(dataset_map) + 1
        dataset_public_id = str(uuid.uuid7())
        dataset_map[dataset] = {"index": dataset_id, "public_id": dataset_public_id}

        cursor.execute(
            f"""INSERT INTO datasets (id, public_id, assembly_id, name) VALUES (
                {dataset_id}, 
                '{dataset_public_id}', 
                {assembly_map[assembly]}, 
                '{dataset}');
            """,
        )

    sample_id = str(uuid.uuid7())

    cursor.execute(
        f"""INSERT INTO sample (id, public_id, dataset_id, technology_id, name, type_id) VALUES (
            {sample_index},
            '{sample_id}', 
            {dataset_map[dataset]['index']},
            {technology_map[technology]}, 
            '{sample}', 
            1);
        """,
    )

    with open(bed, "r") as fin:
        for line in fin:
            line = line.strip()

            # skip bed headers
            if line.startswith("track"):
                continue

            tokens = line.split("\t")
            chr = tokens[0]

            if chr not in CHR_MAP[genome]:
                continue

            chr_id = CHR_MAP[genome][chr]
            start = tokens[1]
            end = tokens[2]
            score = float(tokens[3]) if len(tokens) > 3 else 0

            cursor.execute(
                f"""INSERT INTO regions (sample_id, chr_id, start, end, score) VALUES (
                    {sample_index}, 
                    {chr_id}, 
                    {start}, 
                    {end}, 
                    {score});
                """,
            )

            c += 1

    sample_index += 1


for i, row in df_remote_bigwig_samples.iterrows():
    # insert the remote bigwig samples as well
    dataset_name = row["dataset"]
    sample = row["sample"]
    genome = row["genome"]
    assembly = row["assembly"]
    technology = row["technology"]
    type = row["type"]
    file = row["file"]
    scale = row["scale"]

    if dataset_name not in dataset_map:
        dataset_id = uuid.uuid7()
        dataset = {
            "public_id": str(uuid.uuid7()),
            "index": len(dataset_map) + 1,
            "assembly": assembly_map[assembly],
            "name": dataset_name,
        }

        dataset_map[dataset_name] = dataset

        print(dataset)

        cursor.execute(
            f"""INSERT INTO datasets (id, public_id, assembly_id, name) VALUES (
                {dataset["index"]},
                '{dataset["public_id"]}',
                {dataset["assembly"]},
                '{dataset["name"]}');""",
        )

    dataset = dataset_map[dataset_name]

    with open(file, "r") as f:
        for line in f:
            line = line.strip()
            tokens = line.split(" ")

            if tokens[0] == "track":
                name = tokens[1]

            if tokens[0] == "bigDataUrl":
                url = tokens[1]

                if "bb" not in url and "bigBed" not in url:
                    print("Warning: url does not seem to be a bigBed", url)
                    continue

                id = str(uuid.uuid7())
                cursor.execute(
                    f"""INSERT INTO samples (public_id, dataset_id, name, type_id, url) VALUES (
                    '{id}',
                    {dataset["index"]},
                    '{name}',
                    2,
                    '{url}');
                """,
                )


cursor.execute(
    """INSERT INTO dataset_permissions (dataset_id, permission_id) SELECT id, 1 FROM datasets;"""
)

cursor.execute("CREATE INDEX idx_datasets_name ON datasets(LOWER(name));")
cursor.execute("CREATE INDEX idx_samples_name ON samples(LOWER(name));")

conn.commit()
