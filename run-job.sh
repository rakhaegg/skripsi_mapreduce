FILE_JAR=target/mapreduceService-1.0-SNAPSHOT-jar-with-dependencies.jar
HADOOP_USERNAME=hadoopuser
IP_NAMENODE=192.168.88.2
HOME_DIR=/home/hadoopuser/
NAMA_JAR_TUJUAN=mapreduceService-1.0-SNAPSHOT-jar-with-dependencies.jar
PACKAGE_ID=org.example.mapreduce.App
NAME=/home/hadoopuser/mapreduceService-1.0-SNAPSHOT-jar-with-dependencies.jar

INPUT_FOLDER=/data/"$7.json"
OUTPUT_FOLDER=/output/"$7"

echo ${INPUT_FOLDER}
echo ${OUTPUT_FOLDER}

clear
SCP_ARG="${HADOOP_USERNAME}@${IP_NAMENODE}:${HOME_DIR}${NAMA_JAR_TUJUAN}"
echo "Running SCP"
echo "${SCP_ARG}"
scp $FILE_JAR $SCP_ARG

echo "Type System $1"
echo "Kluster $2"
echo "MinDF $3"
echo "MaxDF $4"
echo "MaxFeature $5"
echo "Max Iteration $6"
echo "Name $7"
echo "UUID $8"

HADOOP_JAR_COMMAND="hadoop jar ${NAME} ${PACKAGE_ID} --input ${INPUT_FOLDER} --output ${OUTPUT_FOLDER} -a $1 -k $2 -mindf $3 -maxdf $4 -f $5 -m $6 -name $7 -uuid $8"
ssh "${HADOOP_USERNAME}@${IP_NAMENODE}" "${HADOOP_JAR_COMMAND}; exit"
echo "selesai"
exit
