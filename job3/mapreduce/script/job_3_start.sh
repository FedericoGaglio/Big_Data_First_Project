./bin/hadoop jar ./streaming/hadoop-streaming-3.2.1.jar \
  -D stream.map.output.field.separator="\t" \
  -D stream.num.map.output.key.fields=2 \
	-D mapreduce.job.reduces=1 \
	-files /Users/alessio/Documents/Universita/big-data/Big_Data_First_Project/dataset/historical_stocks.csv \
	-mapper /Users/alessio/Documents/Universita/big-data/Big_Data_First_Project/job3/mapreduce/mapper.py \
	-reducer /Users/alessio/Documents/Universita/big-data/Big_Data_First_Project/job3/mapreduce/reducer.py \
	-input input/dataset/historical_stock_prices.csv\
	-output /user/alessio/output/result-tmp \
&&
./bin/hadoop jar ./streaming/hadoop-streaming-3.2.1.jar \
  -D stream.map.output.field.separator="\t" \
  -D stream.num.map.output.key.fields=3 \
	-D mapreduce.job.reduces=1 \
	-mapper /Users/alessio/Documents/Universita/big-data/Big_Data_First_Project/job3/mapreduce/mapper2.py \
	-reducer /Users/alessio/Documents/Universita/big-data/Big_Data_First_Project/job3/mapreduce/reducer2.py \
	-input /user/alessio/output/result-tmp/ \
	-output output/result


#The map output keys of the above Map/Reduce job normally have four fields separated by “.”. However, the Map/Reduce framework will partition the map outputs by the first two fields of the keys using the -D mapred.text.key.partitioner.options=-k1,2 option

#OLD
./bin/hadoop jar ./streaming/hadoop-streaming-3.2.1.jar \
	-D stream.num.map.output.key.fields=2 \
	-D stream.reduce.input.field.separator=, \
	-D mapreduce.job.reduces=1 \
	-files /Users/alessio/Documents/Universita/big-data/Big_Data_First_Project/dataset/historical_stocks.csv \
	-mapper /Users/alessio/Documents/Universita/big-data/Big_Data_First_Project/job3/mapreduce/mapper.py \
	-reducer /Users/alessio/Documents/Universita/big-data/Big_Data_First_Project/job3/mapreduce/reducer.py \
	-input input/dataset/historical_stock_prices.csv\
	-output /user/alessio/output/result-tmp \
	-partitioner org.apache.hadoop.mapred.lib.KeyFieldBasedPartitioner \
&&
./bin/hadoop jar ./streaming/hadoop-streaming-3.2.1.jar \
	-D stream.num.map.output.key.fields=3 \
	-D stream.reduce.input.field.separator=, \
	-D mapreduce.partition.keypartitioner.options=-k1,3n \
	-D mapreduce.job.reduces=1 \
	-mapper /Users/alessio/Documents/Universita/big-data/Big_Data_First_Project/job3/mapreduce/mapper2.py \
	-reducer /Users/alessio/Documents/Universita/big-data/Big_Data_First_Project/job3/mapreduce/reducer2b.py \
	-input /user/alessio/output/result-tmp/ \
	-output output/result
