./bin/hadoop jar ./streaming/hadoop-streaming-3.2.1.jar \
	-D stream.num.map.output.key.fields=2 \
	-D mapreduce.partition.keypartitioner.options=-k1,1 \
	-D mapreduce.job.reduces=1 \
	-files /Users/alessio/Desktop/job3b_test_local.csv \
	-mapper python/mapper.py \
	-reducer python/reducer.py \
	-input /user/alessio/input/job3_test_local.csv\
	-output /user/alessio/output/result-tmp \
	-partitioner org.apache.hadoop.mapred.lib.KeyFieldBasedPartitioner \
	&&
	./bin/hadoop jar ./streaming/hadoop-streaming-3.2.1.jar \
	-D stream.num.map.output.key.fields=3 \
	-D mapreduce.job.reduces=1 \
	-mapper python/mapper2.py \
	-reducer python/reducer2.py \
	-input /user/alessio/output/result-tmp/ \
	-output output/result \

