genome=hg19
dir=/home/antony/development/data/modules/beds/${genome}/ChIP-seq
 
rm ${dir}/bigbed.sql

python trackdbtosql.py --file="/ifs/scratch/cancer/Lab_RDF/ngs/chip_seq/data/human/rdf/hg19/rdf/elodie_dlbcl_cell_lines_29/analysis/hub_elodie_cell_lines_peaks/hg19/trackDb.txt" \
    --dataset="RDF 29CL Peaks" \
    --genome=${genome} \
    --out="${dir}/bigbed.sql"
