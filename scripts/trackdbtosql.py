# -*- coding: utf-8 -*-
"""
Encode read counts per base in 2 bytes

@author: Antony Holmes
"""
import argparse

import uuid_utils as uuid

parser = argparse.ArgumentParser()
parser.add_argument("-f", "--file", help="trackdb file")
parser.add_argument("-d", "--dataset", help="dataset")
parser.add_argument("-p", "--platform", default="ChIP-seq", help="platform")
parser.add_argument(
    "-g", "--genome", default="Human", help="genome sample was aligned to"
)
parser.add_argument("-a", "--assembly", default="hg19", help="assembly version")

parser.add_argument("-o", "--out", help="output directory")
args = parser.parse_args()

file = args.file
dataset = args.dataset  # sys.argv[1]
genome = args.genome  # sys.argv[3]
assembly = args.assembly  # sys.argv[4]
platform = args.platform


out = args.out

print(out)

with open(out, "w") as fout:

    dataset_id = uuid.uuid7()
    print("BEGIN TRANSACTION;", file=fout)

    print(
        f"""INSERT INTO datasets (id, genome, assembly, platform, name) VALUES ('{dataset_id}', '{genome}', '{assembly}', '{platform}', '{dataset}');""",
        file=fout,
    )

    print(
        f"INSERT INTO dataset_permissions (dataset_id, permission_id) VALUES ('{dataset_id}', '019c05b1-f0e7-7107-82d0-27bac005f103');",
        file=fout,
    )

    print("COMMIT;", file=fout)

    print("BEGIN TRANSACTION;", file=fout)
    with open(file, "r") as f:
        for line in f:
            line = line.strip()
            tokens = line.split(" ")

            if tokens[0] == "track":
                name = tokens[1]

            if tokens[0] == "bigDataUrl":
                url = tokens[1]
                publicId = uuid.uuid7()
                # generate("0123456789abcdefghijklmnopqrstuvwxyz", 12)
                print(
                    f"INSERT INTO samples (id, dataset_id, name, type, regions, url, tags) VALUES ('{publicId}', '{dataset_id}', '{name}', 'Remote BigBed', 0, '{url}', '');",
                    file=fout,
                )
    print("COMMIT;", file=fout)
