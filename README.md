# Data Validation Tool

This is a start for a simple command line data validation tool for Hive tables based on Spark, implemented in Scala and
built using sbt.

**This is very early, not ready for prime time version 0.1**

Right now all it does is, comparing the counts in raw and current table pairs
for a given Hive database. besides the single parameter for the database name,
it requires the following config paramters, as a file named

  `application.json`

in the directory you submit the spark job, with the following config values:
```
{
    "hCatServer": "127.0.0.1",
    "hCatPort": 50111,
    "hCatUser": "hcat",
    "rawTablePostFix": "_raw",
    "currentTablePostFix": "_current"
}
```

## Build an Deploy

`cd <your_path>/dataValidationTool/`

`mvn clean assembly`

The jar got createt at:

`ls -l target/scala-2.10/dataValidationTool-assembly-0.1.jar`

Copy the jar file and the application.json file to your edge node:

```
scp target/scala-2.10/dataValidationTool-assembly-0.1.jar <edgenode>
scp application.json <edgenode>
```

## Run

on the edge node, submit the spark job as fillows:

`spark-submit --master yarn --class "com.thinkbiganalytics.datavalidationtool.Main" --files log4j.properties  <path>/dataValidationTool-assembly-0.1.jar default`

If everything went well, you should see the counts in tabular form:

```
+------------+--------------+--------+------------------+------------+-----------+
|databaseName|      rawTable|rawCount|      currentTable|currentCount|countsMatch|
+------------+--------------+--------+------------------+------------+-----------+
|     default|small_test_raw|       3|small_test_current|           1|      false|
+------------+--------------+--------+------------------+------------+-----------+
```

## Todo

Where should I start ??? I mean, this is the 0.1 version of a data validation tool,
with just 150 lines Scala of code, all it does is comparing counts.

- **Write Unit tests** (refactor code for testability)
- Test the code on a cluster
- add some real validation, just comparing counts is not good enough
- make it run locally
- think paralellization thru
- provide updates on progress as it runs
- add helper class to submit the job from within code (Pipeline Controller usecase)





