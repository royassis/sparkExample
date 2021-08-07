## start 
# start-dfs.sh && start-yarn.sh && start-master.sh && start-worker.sh localhost:7077 && current_dir=$(pwd) && cd /home/roy/apache-hive-3.1.2-bin && hive --service metastore & && hive --service hiveserver2 &
start-dfs.sh
start-yarn.sh
start-master.sh
start-worker.sh localhost:7077
current_dir=$(pwd)
cd $HIVE_HOME
hive --service metastore &
hive --service hiveserver2 --hiveconf hive.root.logger=INFO,console &
cd $current_dir

# stop
kill_pid_by_name metastore hiveserver2 
stop-master.sh
stop-worker.sh
stop-yarn.sh
stop-dfs.sh

