# -*- coding: utf-8 -*-
"""
Encode read counts per base in 2 bytes

@author: Antony Holmes
"""
import argparse
import sys
 
parser = argparse.ArgumentParser()
parser.add_argument("-s", "--sample", help="sample name")
parser.add_argument("-b", "--bed", help="ned file")
parser.add_argument(
    "-p", "--platform", default="ChIP-seq", help="data plaform"
)
parser.add_argument(
    "-g", "--genome", default="hg19", help="genome sample was aligned to"
)
 
parser.add_argument("-o", "--out", help="output file")
args = parser.parse_args()

sample = args.sample  # sys.argv[1]
bed = args.bed  # sys.argv[2]
platform = args.platform
genome = args.genome  # sys.argv[3]
 
out  = args.out

with open(out, "w") as f:
    public_id = ':'.join([genome, platform, sample])
    print("BEGIN TRANSACTION;", file=f)
    print(f"INSERT INTO info (public_id, genome, platform, name) VALUES ('{public_id}', '{genome}', '{platform}', '{sample}');", file=f)
    print("COMMIT;", file=f)

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
            score = tokens[3] if len(tokens)>3 else ""

            if score != "":
                print(f"INSERT INTO bed (chr, start, end, score) VALUES ('{chr}',{start},{end}, {score});", file=f)
            else:
                print(f"INSERT INTO bed (chr, start, end) VALUES ('{chr}',{start},{end});", file=f)

    print("COMMIT;", file=f)