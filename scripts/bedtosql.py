# -*- coding: utf-8 -*-
"""
Encode read counts per base in 2 bytes

@author: Antony Holmes
"""
import argparse
import sys

import uuid_utils as uuid
from nanoid import generate

parser = argparse.ArgumentParser()
parser.add_argument("-s", "--sample", help="sample name")
parser.add_argument("-b", "--bed", help="bed file")
parser.add_argument("-p", "--platform", default="ChIP-seq", help="data plaform")
parser.add_argument(
    "-g", "--genome", default="Human", help="genome sample was aligned to"
)

parser.add_argument("-a", "--assembly", default="hg19", help="assembly version")

parser.add_argument("-o", "--out", help="output file")
args = parser.parse_args()

sample = args.sample  # sys.argv[1]
bed = args.bed  # sys.argv[2]
platform = args.platform
genome = args.genome  # sys.argv[3]
assembly = args.assembly  # sys.argv[4]

out = args.out

with open(out, "w") as f:
    public_id = uuid.uuid7()  # generate("0123456789abcdefghijklmnopqrstuvwxyz", 12)

    c = 0

    print("BEGIN TRANSACTION;", file=f)
    with open(bed, "r") as fin:
        for line in fin:
            line = line.strip()

            # skip bed headers
            if line.startswith("track"):
                continue

            tokens = line.split("\t")
            chr = tokens[0]
            start = tokens[1]
            end = tokens[2]
            score = tokens[3] if len(tokens) > 3 else ""

            if score != "":
                print(
                    f"INSERT INTO regions (chr, start, end, score) VALUES ('{chr}',{start}, {end}, {score});",
                    file=f,
                )
            else:
                print(
                    f"INSERT INTO regions (chr, start, end) VALUES ('{chr}', {start}, {end});",
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
