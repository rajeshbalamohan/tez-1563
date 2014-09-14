To compile:
-----------
mvn clean package

To run:
-------
1. Just place some simple text file in /tmp/m1.out
2. HADOOP_CLASSPATH=$TEZ_CONF_DIR:$TEZ_HOME/*:$TEZ_HOME/lib/*:$HADOOP_CLASSPATH yarn jar ./target/benchmark-1.0-SNAPSHOT.jar org.apache.tez.benchmark.SessionTest /tmp/m1.out /tmp/m2.out
