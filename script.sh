cd docker-hadoop-master
docker-compose up -d
id=$(docker ps | grep namenode | AWK '{printf $1}')
docker cp ../target/lab1-1.0-SNAPSHOT-jar-with-dependencies.jar $id:lab1-1.0-SNAPSHOT-jar-with-dependencies.jar
docker cp ../../input/000000 $id:000000
docker cp ../hadoop.sh $id:hadoop.sh
winpty docker exec namenode bash "hadoop.sh"