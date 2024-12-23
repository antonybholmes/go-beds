dir=data/modules/beds

python discover_beds.py --dir=${dir} 

 
rm ${dir}/tracks.db
cat tracks.sql | sqlite3 ${dir}/tracks.db
cat ${dir}/tracks.sql | sqlite3 ${dir}/tracks.db
