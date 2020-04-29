./bin/hadoop jar ./streaming/hadoop-streaming-3.2.1.jar \
	-D stream.num.map.output.key.fields=2 \
	-D mapreduce.partition.keypartitioner.options=-k1,1 \
	-D mapreduce.job.reduces=1 \
	-files /Users/alessio/Documents/Università/big-data/Big_Data_First_Project/dataset/job3_test.csv \
	-mapper /Users/alessio/Documents/Università/big-data/Big_Data_First_Project/job3/mapreduce/mapper.py \
	-reducer /Users/alessio/Documents/Università/big-data/Big_Data_First_Project/job3/mapreduce/reducer.py \
	-input input/dataset/job3_test_price.csv\
	-output /user/alessio/output/result-tmp \
	-partitioner org.apache.hadoop.mapred.lib.KeyFieldBasedPartitioner \
&&
./bin/hadoop jar ./streaming/hadoop-streaming-3.2.1.jar \
	-D stream.num.map.output.key.fields=3 \
	-D mapreduce.job.reduces=1 \
	-mapper /Users/alessio/Documents/Università/big-data/Big_Data_First_Project/job3/mapreduce/mapper2.py \
	-reducer /Users/alessio/Documents/Università/big-data/Big_Data_First_Project/job3/mapreduce/reducer2.py \
	-input /user/alessio/output/result-tmp/ \
	-output output/result \
