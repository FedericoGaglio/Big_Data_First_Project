INPUT_NAME=
INPUT_2_NAME=

hdfs dfs -mkdir /input

hdfs dfs -put $INPUT_NAME /input

hadoop jar hadoop-streaming-3.2.1.jar \
	-D stream.map.output.field.separator="\t" \
	-D stream.num.map.output.key.fields=3 \
	-D mapreduce.job.reduces=1 \
	-files $INPUT_2_NAME \
	-mapper mapper.py \
	-reducer reducer.py \
	-input /input/$INPUT_NAME \
	-output /output/result \
	-file mapper.py \
	-file reducer.py

# hdfs dfs -cat /output/result/part-00000
# hdfs dfs -rm -r -f /output

