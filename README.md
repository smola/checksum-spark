
# checksum-spark [![Build Status](https://travis-ci.com/smola/checksum-spark.svg?branch=master)](https://travis-ci.com/smola/checksum-spark) [![Release](https://jitpack.io/v/io.mola/checksum-spark_2.11.svg)](https://jitpack.io/#io.mola/checksum-spark_2.11) [![codecov](https://codecov.io/gh/smola/checksum-spark/branch/master/graph/badge.svg)](https://codecov.io/gh/smola/checksum-spark)

**checksum-spark** verifies and creates checksum files using Apache Spark.

Design goals:

* Keep CLI as close to [GNU coreutils](https://github.com/smola/checksum-spark) (e.g. [md5sum](http://www.gnu.org/software/coreutils/md5sum)) as possible.
* Easy to use with HDFS or any storage supported by Apache Spark.

## Download

### Releases

Check the [releases page](https://github.com/smola/checksum-spark/releases) for the latest version.

### Via Maven repository

checksum-spark is published to [jitpack](https://jitpack.io/). You can fetch it with [Maven](http://maven.apache.org/):

```
mvn org.apache.maven.plugins:maven-dependency-plugin:2.1:get -DrepoUrl=https://jitpack.io -Dartifact=io.mola:checksum-spark_2.11:v0.2.0
```

Or with [Coursier](http://get-coursier.io):

```
coursier fetch --repository https://jitpack.io io.mola:checksum-spark_2.11:v0.2.0,classifier=assembly
```

# License

Copyright © 2018 Santiago M. Mola

This project is released under the terms of the Apache License 2.0.

