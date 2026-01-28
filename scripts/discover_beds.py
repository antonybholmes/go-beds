# -*- coding: utf-8 -*-
"""
Encode read counts per base in 2 bytes

@author: Antony Holmes
"""
import argparse
import os
import sqlite3

import uuid_utils as uuid
from nanoid import generate

genome_map = {
    "hg19": "Human",
    "hg38": "Human",
    "grch38": "Human",
    "mm10": "Mouse",
    "rn6": "Rat",
}

parser = argparse.ArgumentParser()
parser.add_argument("-d", "--dir", help="dir to search")
args = parser.parse_args()

dir = args.dir  # sys.argv[1]

data = []

datasets = {}

for root, dirs, files in os.walk(dir):
    for filename in files:
        if filename == "samples.db" or filename == "tracks.db":
            continue

        if filename.endswith(".db"):
            sample = os.path.splitext(filename)[0]
            relative_dir = root.replace(dir, "")[1:]
            print(root, filename)
            assembly, platform, dataset_name = relative_dir.split("/")
            # filepath = os.path.join(root, filename)
            print(sample, os.path.join(root, filename))

            genome = genome_map.get(assembly.lower(), assembly)

            dataset_name = dataset_name.replace("_", " ")

            if dataset_name not in datasets:
                dataset_id = uuid.uuid7()
                datasets[dataset_name] = {
                    "id": dataset_id,
                    "name": dataset_name,
                    "platform": platform,
                    "genome": genome,
                    "assembly": assembly,
                }

            dataset = datasets[dataset_name]

            conn = sqlite3.connect(os.path.join(root, filename))
            conn.row_factory = sqlite3.Row

            # Create a cursor object
            cursor = conn.cursor()

            # Execute a query to fetch data
            cursor.execute("SELECT id, name, type, regions FROM sample")

            # Fetch all results
            results = cursor.fetchall()

            # Print the results
            for row in results:
                # row = list(row)
                # row.append(generate("0123456789abcdefghijklmnopqrstuvwxyz", 12))

                # row.append(os.path.join(relative_dir, filename))

                row = {
                    "id": row["id"],
                    "name": row["name"],
                    "regions": row["regions"],
                    "dataset_id": dataset["id"],
                    "type": "Seq",
                    "url": os.path.join(relative_dir, filename),
                }

                data.append(row)

            conn.close()

with open(os.path.join(dir, "samples.sql"), "w") as f:
    print("BEGIN TRANSACTION;", file=f)
    for [dataset_name, dataset] in datasets.items():

        print(
            f"""INSERT INTO datasets (id, genome, assembly, platform, name) VALUES (
                '{dataset["id"]}',
                '{dataset["genome"]}',
                '{dataset["assembly"]}',
                '{dataset["platform"]}',
                '{dataset["name"]}');""",
            file=f,
        )

        # default rdf:view permission
        print(
            f"INSERT INTO dataset_permissions (dataset_id, permission_id) VALUES ('{dataset["id"]}', '019c05b1-f0e7-7107-82d0-27bac005f103');",
            file=f,
        )

    print("COMMIT;", file=f)

    print("BEGIN TRANSACTION;", file=f)
    for row in data:

        print(
            f"INSERT INTO samples (id, dataset_id, name, type, regions, url) VALUES ('{row["id"]}', '{row["dataset_id"]}', '{row["name"]}', '{row["type"]}', {row["regions"]}, '{row["url"]}');",
            file=f,
        )

    print("COMMIT;", file=f)
