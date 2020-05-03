./bin/hadoop jar ./streaming/hadoop-streaming-3.2.1.jar \
	-D stream.num.map.output.key.fields=2 \
	-D mapreduce.job.reduces=1 \
	-mapper /Users/alessio/Documents/Università/big-data/Big_Data_First_Project/job1/mapreduce/mapper.py \
	-reducer /Users/alessio/Documents/Università/big-data/Big_Data_First_Project/job1/mapreduce/reducer.py \
	-input input/dataset/job1_test_price.csv\
	-output output/result

