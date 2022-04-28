# DS256_Project

## Range Search Tree

Build a jar file using:
```
mvn package
```
Copy this jar file to cluster

Run jar file on spark cluster using:
```
spark-submit --class RangeSearch --master yarn --num-executors 2 --driver-memory 512m --executor-memory 1G --executor-cores 1 RangeSearchTree-1.0-SNAPSHOT.jar <hdfs-path-input-file> <hdfs-path-output-file> <hdfs-path-query-file>
```
