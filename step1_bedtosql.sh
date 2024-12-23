genome=hg19
dir=data/modules/beds/ChIP-seq

cat beds.txt | sed 1d | while read bed
do
    sample=`echo ${bed} | sed -r 's/\..+//'` #Peaks_CB4_BCL6_RK040_vs_Input_RK063_p12
    out=${dir}/${genome}/${sample}.sql 
    #bed=Peaks_CB4_BCL6_RK040_vs_Input_RK063_p12.bed
    python bedtosql.py --sample=${sample} --bed=${bed} --genome=${genome} --out=${out}
    ./step2_create_db.sh ${out}  
done 
