INPUT_NAME=historical_stock_prices.csv

hdfs dfs -mkdir /input

hdfs dfs -put $INPUT_NAME /input

hadoop jar hadoop-streaming-3.2.1.jar \
	-D stream.num.map.output.key.fields=2 \
	-D mapreduce.job.reduces=1 \
	-mapper mapper.py \
	-reducer reducer.py \
	-input /input/$INPUT_NAME \
	-output /user/hadoop/output/result \
	-file mapper.py \
	-file reducer.py

