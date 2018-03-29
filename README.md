to build and run sbt project:  
  
*** FOR: helloWorld ***  
1. cd into project folder: helloWorld  
2. sbt run  
  
*** FOR: sparkHelloWorld ***  
1. cd into project folder: sparkHelloWorld  
2. create jar file: sbt package  
  
3. start spark master-slave using default value  
$SPARK_HOME/bin/spark-submit target/scala-2.11/spark-hello-world_2.11-1.0.jar MYOUTPUT  
...where MYOUTPUT is an arg where data is written. Note that spark will NOT overwrite if  
...the same name is present, so change everytime or integrate datetime for uniqueness.  
...also note to cp $SPARK_HOME/conf/log4j.properties.template $SPARK_HOME/conf/log4j.properties  
  
4. start spark master-slave using all cores  
$SPARK_HOME/bin/spark-submit --master "local[*]" target/scala-2.11/spark-hello-world_2.11-1.0.jar MYOUTPUT  
  
5. start a spark cluster of one and submit job to existing master:  
$SPARK_HOME/sbin/start-master.sh  
go to web browser: localhost:8080  
get URL: i.e. spark://YOUR-COMPS-NAME.local:7077  
$SPARK_HOME/sbin/start-slave.sh spark://YOUR-COMPS-NAME.local:7077  
submit job to cluster:  
$SPARK_HOME/bin/spark-submit --master spark://YOUR-COMPS-NAME.local:7077  
  
*** FOR: sparkRandomForest ***  
1. meant to be ran in aws
