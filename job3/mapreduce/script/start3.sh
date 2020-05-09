INPUT_3_NAME=

hdfs dfs -mkdir /input

hdfs dfs -put $INPUT_NAME /input

hadoop jar hadoop-streaming-3.2.1.jar \
	-D stream.map.output.field.separator="\t" \
  -D stream.num.map.output.key.fields=2 \
	-D mapreduce.job.reduces=1 \
	-files $INPUT_3_NAME \
	-mapper mapper.py \
	-reducer reducer.py \
	-input /input/$INPUT_NAME \
	-output /user/hadoop/output/result-tmp \
	-file mapper.py \
	-file reducer.py \
&&
hadoop jar hadoop-streaming-3.2.1.jar \
	-D stream.map.output.field.separator="\t" \
  -D stream.num.map.output.key.fields=3 \
	-D mapreduce.job.reduces=1 \
	-mapper mapper2.py \
	-reducer reducer2.py \
	-input /user/hadoop/output/result-tmp/ \
	-output /user/hadoop/output/result \
	-file mapper2.py \
	-file reducer2.py