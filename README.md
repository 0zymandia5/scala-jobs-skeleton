# Scala-Jobs-Skeleton

Simple skeleton for prototype of jobs execution on scala + spark, the jobs are mapped on **menu.csv** file at ***src/main/behaviors*** path.

The jobs are mapped mainly by topic, then by name, so If you don know the job targeted to be excetuted you must run:

```
sbt:> run
```

then the menu.csv will be displayed in order you choose the **TopicType**, then the **JobID**.

If you already know the the **TopicType** and the **JobID** you can take advantage and type those as parameters:

```
sbt:> run 1 1
```

## Requirements

Install [sbt](https://www.scala-sbt.org/download/)
