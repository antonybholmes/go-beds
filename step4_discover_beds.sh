dir=data/modules/beds

python discover_beds.py --dir=${dir} 

 
rm ${dir}/beds.db
cat beds.sql | sqlite3 ${dir}/beds.db
cat ${dir}/beds.sql | sqlite3 ${dir}/beds.db
