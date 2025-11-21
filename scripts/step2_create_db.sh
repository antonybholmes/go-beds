# for f in `ls *.sql | grep -v table`
# do
#     name=`echo ${f} | sed -r 's/.sql//'`
#     rm ${name}.db
#     cat tables.sql | sqlite3 ${name}.db
#     cat ${f} | sqlite3 ${name}.db
# done

sample=$1
db=`echo ${sample} | sed -r 's/sql/db/'`

rm ${db}
cat bed.sql | sqlite3 ${db}
cat ${sample} | sqlite3 ${db}


 
