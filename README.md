# SparkScalaStarterProject


# RDD

# Dataframes: (working with structured data)
<ul>
<li>Contains Row objects</li>
<li>Can Run SQL queries</li>
<li>Has a schema (leading to more efficient storage)</li>
<li>Read and write to JSON, Hive, parquet</li>
<li>Communicate with JDBC/ODBC, Tableau</li>
</ul>


# Datsets: 


# Spark Concepts

Accumulator  allows many executors to increment a shared variables


# Submit Spark Job

# How do you run your spark job periodically?

<p>There a number of options for scheduling jobs periodically and keeping track of inter-job dependencies, this is far from a comprehensive list:</p>
<ul>
<li>Cron: Old school but it still works well</li>
<li>Chronos: A job scheduler on top of Mesos created by AirBnB</li>
<li>Oozie: A job scheduler on top of Hadoop, a very mature project however the xml job configurations can get painful and it has certain quirky limitations</li>
<li>Azkaban: A job scheduler originally written for Hadoop by LinkedIn</li>
<li>Airflow: Another job scheduler written by AirBnb, this one is based on Python and is quiet flexible</li>
<li>Luigi: A job scheduler written by Spotify</li>
</ul>
<p>My personal preference right now is Airflow since it can be deployed with a very lightweight configuration while allowing for some solid power in it’s configurations.</p>

# Running Spark on a cluster
In this chapter we will talk about  running spark on a real cluster.
Knowledge you need for actually running spark in a real production setting at large scale.
### Using spark-submit to run Spark driver scripts
In our use case we will try to run spark on AWS EMR, tuning performance on a cluster.

Packaging and deploying your application : 
1. Make sure there are no paths to your local filesystem used in your script ! That's what HDFS, S3, etc . are for.
2. Package up your Scala project into a JAR file
3. You can now use spark-submit to execute your drivers script outside of the IDE
4. spark-submit --class <class object that contains your main function> --jars <path to any dependencies> --files <files you want placed alongside your application> <your JAR file>

POC :
/Users/atif/Tools/spark-3.2.1-bin-hadoop3.2/bin/spark-submit --class  courses.HelloWorld  /Users/atif/Projects/ATIF/SparkScalaStarterProject/out/artifacts/SparkCourse/SparkCourse.jar
/Users/atif/Tools/spark-3.2.1-bin-hadoop2.7/bin/spark-submit --class  courses.HelloWorld  /Users/atif/Projects/ATIF/SparkScalaStarterProject/out/artifacts/SparkCourse/SparkCourse.jar

# Packaging dri