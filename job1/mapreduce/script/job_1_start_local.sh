./bin/hadoop jar ./streaming/hadoop-streaming-3.2.1.jar \
  -D stream.map.output.field.separator="\t" \
	-D stream.num.map.output.key.fields=2 \
	-D mapreduce.job.reduces=1 \
	-mapper /Users/alessio/Documents/Universita/big-data/Big_Data_First_Project/job1/mapreduce/mapper.py \
	-reducer /Users/alessio/Documents/Universita/big-data/Big_Data_First_Project/job1/mapreduce/reducer.py \
	-input input/dataset/historical_stock_prices.csv\
	-output output/result

