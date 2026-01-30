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

CHR_MAP = {chr: idx + 1 for idx, chr in enumerate(HUMAN_CHRS)}

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

with open(out, "w") as f:
    print("BEGIN TRANSACTION;", file=f)

    chrs = HUMAN_CHRS if genome.lower() == "human" else MOUSE_CHRS
    chr_map = {chr: idx + 1 for idx, chr in enumerate(chrs)}

    for chr in chrs:
        print(
            f"INSERT INTO chromosomes (id, name) VALUES ({chr_map[chr]}, '{chr}');",
            file=f,
        )
    print("COMMIT;", file=f)

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
