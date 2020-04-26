./bin/hadoop jar ./streaming/hadoop-streaming-3.2.1.jar \
	-D stream.num.map.output.key.fields=2 \
	-D mapreduce.job.reduces=1 \
	-mapper python/mapper.py \
	-reducer python/reducer.py \
	-input input/job1_test_local.csv\
	-output output/result

