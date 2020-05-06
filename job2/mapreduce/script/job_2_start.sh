./bin/hadoop jar ./streaming/hadoop-streaming-3.2.1.jar \
	-D stream.num.map.output.key.fields=3 \
	-D mapreduce.partition.keypartitioner.options=-k1,1 \
	-D mapreduce.job.reduces=1 \
	-files /Users/alessio/Documents/Universita/big-data/Big_Data_First_Project/dataset/historical_stocks.csv \
	-mapper /Users/alessio/Documents/Universita/big-data/Big_Data_First_Project/job2/mapreduce/mapper.py \
	-reducer /Users/alessio/Documents/Universita/big-data/Big_Data_First_Project/job2/mapreduce/reducer.py \
	-input input/dataset/historical_stock_prices.csv\
	-output output/result \
	-partitioner org.apache.hadoop.mapred.lib.KeyFieldBasedPartitioner

