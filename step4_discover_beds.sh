dir=data/modules/beds

python discover_beds.py --dir=${dir}
echo "BEGIN TRANSACTION;">> ${dir}/tracks.sql
cat /home/antony/development/data/modules/beds/hg19/ChIP-seq/bigbed.sql >> ${dir}/tracks.sql
echo "COMMIT;">> ${dir}/tracks.sql
 
rm ${dir}/tracks.db
cat tracks.sql | sqlite3 ${dir}/tracks.db
cat ${dir}/tracks.sql | sqlite3 ${dir}/tracks.db
