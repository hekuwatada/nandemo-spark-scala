# Namdemo Spark with Scala
Templates, patterns, experiments with Scala and Spark

## Troubleshooting

java.io.IOException: Could not locate executable null\bin\winutils.exe in the Hadoop binaries.

Observed when running unit tests with Spark local and RDD on Windows (by necessity).

Solution: 
1. download and install winutils.exe to /path/to/winutils
2. set HADOOP_HOME to /path/to/winutils

scala.ScalaReflectionException: class java.sql.Timestamp in JavaMirror with ClasspathFilter

Observed when running unit tests with Spark local and Dataset. The issue appears to have been due to a newer version of sbt. It may also be to do with a newer version of JDK.

Solution: downgraded sbt to v0.13.x