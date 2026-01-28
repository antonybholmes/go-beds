dir=../data/modules/beds

python discover_beds.py --dir=${dir}

cat ../data/modules/beds/hg19/ChIP-seq/bigbed.sql >> ${dir}/samples.sql
 
rm ${dir}/samples.db
cat samples.sql | sqlite3 ${dir}/samples.db
cat ${dir}/samples.sql | sqlite3 ${dir}/samples.db