# -*- coding: utf-8 -*-
"""
Encode read counts per base in 2 bytes

@author: Antony Holmes
"""
import argparse
import os
import sqlite3
from nanoid import generate

parser = argparse.ArgumentParser()
parser.add_argument("-d", "--dir", help="dir to search")
args = parser.parse_args()

dir = args.dir  # sys.argv[1]

data = []

for root, dirs, files in os.walk(dir):
    for filename in files:
        if filename == "tracks.db":
            continue

        if filename.endswith(".db"):
            sample = os.path.splitext(filename)[0]
            relative_dir = root.replace(dir, "")[1:]
            genome, platform, dataset = relative_dir.split("/")
            # filepath = os.path.join(root, filename)
            print(sample, os.path.join(root, filename))

            dataset = dataset.replace("_", " ")

            conn = sqlite3.connect(os.path.join(root, filename))

            # Create a cursor object
            cursor = conn.cursor()

            # Execute a query to fetch data
            cursor.execute(
                "SELECT public_id, genome, platform, name, track_type, regions FROM track"
            )

            # Fetch all results
            results = cursor.fetchall()

            # Print the results
            for row in results:
                row = list(row)
                # row.append(generate("0123456789abcdefghijklmnopqrstuvwxyz", 12))
                row.append(dataset)
                row.append(os.path.join(relative_dir, filename))
                row.append(dataset)
                data.append(row)

            conn.close()

with open(os.path.join(dir, "tracks.sql"), "w") as f:
    print("BEGIN TRANSACTION;", file=f)
    for row in data:
        values = ", ".join([f"'{v}'" for v in row])
        print(
            f"INSERT INTO tracks (public_id, genome, platform, name, track_type, regions, dataset, url, tags) VALUES ({values});",
            file=f,
        )

    print("COMMIT;", file=f)
