## hadoop
start-dfs.sh  
start-yarn.sh  
stop-dfs.sh  
stop-yarn.sh  

## hive
!connect jdbc:hive2://localhost:10000/default  
schematool -initSchema -dbType derby  
hive --service hiveserver2 --hiveconf hive.root.logger=INFO,console  
hive --service metastore  
rm -r derby.log metastore_db

## spark
start-master.sh   
start-worker.sh localhost:7077  
stop-master.sh   
stop-worker.sh   

## dirs
# spark  
cd /home/roy/spark-3.1.2-bin-hadoop3.2
# hive  
cd /home/roy/apache-hive-3.1.2-bin
# hadoop  
cd /home/roy/hadoop-3.3.1
