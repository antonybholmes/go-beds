genome=hg19
dir=data/modules/beds/${genome}/ChIP-seq

cat beds3.txt | sed 1d | while read bed
do
    	sample=`basename ${bed} | sed -r 's/\..+//' | sed -r 's/Peaks_//'` #Peaks_CB4_BCL6_RK040_vs_Input_RK063_p12
    	out=${dir}/RDF_Lab/${sample}.sql 
    	echo ${sample} ${bed}
	    #bed=Peaks_CB4_BCL6_RK040_vs_Input_RK063_p12.bed
		python bedtosql.py --sample=${sample} --bed=${bed} --genome=${genome} --out=${out}
    	./step2_create_db.sh ${out}
	#break
done 
