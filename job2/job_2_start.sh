./bin/hadoop jar ./streaming/hadoop-streaming-3.2.1.jar \
	-D stream.num.map.output.key.fields=3 \
	-D mapreduce.partition.keypartitioner.options=-k1,1 \
	-D mapreduce.job.reduces=1 \
	-files /Users/alessio/Documents/Università/big-data/Big_Data_First_Project/job2/job2b_test_local.csv \
	-mapper python/mapper.py \
	-reducer python/reducer.py \
	-input input/job2_test_local.csv\
	-output output/result \
	-partitioner org.apache.hadoop.mapred.lib.KeyFieldBasedPartitioner


	./bin/hadoop jar ./streaming/hadoop-streaming-3.2.1.jar \
	-D stream.num.map.output.key.fields=3 \
	-D mapreduce.partition.keypartitioner.options=-k1,1 \
	-D mapreduce.job.reduces=1 \
	-files /Users/alessio/Downloads/daily-historical-stock-prices-1970-2018/historical_stocks_1.csv \
	-mapper python/mapper.py \
	-reducer python/reducer.py \
	-input input/job2_test_local.csv\
	-output output/result \
	-partitioner org.apache.hadoop.mapred.lib.KeyFieldBasedPartitioner

