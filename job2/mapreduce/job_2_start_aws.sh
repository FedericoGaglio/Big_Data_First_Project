PEM_FILE=/Users/alessio/Documents/Università/big-data/alessio_bigdata.pem
PATH_STREAMING=/Users/alessio/Documents/Università/big-data/hadoop-3.2.1/streaming/hadoop-streaming-3.2.1.jar
MAPPER=/Users/alessio/Documents/Università/big-data/Big_Data_First_Project/job2/mapreduce/mapper.py
REDUCER=/Users/alessio/Documents/Università/big-data/Big_Data_First_Project/job2/mapreduce/reducer.py
INPUT_PATH=/Users/alessio/Documents/Università/big-data/Big_Data_First_Project/dataset/
START=/Users/alessio/Documents/Università/big-data/Big_Data_First_Project/job2/mapreduce/start2.sh


INPUT_NAME=job2_test_price.csv
INPUT_2_NAME=job2_test.csv
CLUSTER_ADDRESS=hadoop@ec2-35-173-47-49.compute-1.amazonaws.com

scp -i $PEM_FILE $PATH_STREAMING $CLUSTER_ADDRESS:~
scp -i $PEM_FILE $MAPPER $CLUSTER_ADDRESS:~
scp -i $PEM_FILE $REDUCER $CLUSTER_ADDRESS:~
scp -i $PEM_FILE $INPUT_PATH$INPUT_NAME $CLUSTER_ADDRESS:~
scp -i $PEM_FILE $INPUT_PATH$INPUT_2_NAME $CLUSTER_ADDRESS:~
scp -i $PEM_FILE $START $CLUSTER_ADDRESS:~

ssh -i $PEM_FILE $CLUSTER_ADDRESS

