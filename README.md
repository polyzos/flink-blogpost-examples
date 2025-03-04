Flink Blogpost Examples
-----------------------

This repository contains the examples used for my Flink blogposts @ [dev.to](https://dev.to/ipolyzos).

## Blog Posts
### [A Deep Dive Into Apache Flink Timers](https://dev.to/ipolyzos/a-deep-dive-into-apache-flink-timers-6m4) 
✅ Code examples can be found [here](https://github.com/polyzos/flink-blogpost-examples/tree/main/src/main/java/io/ipolyzos/timers).

###  [Understanding Custom Triggers In Apache Flink](https://dev.to/ipolyzos/understanding-custom-triggers-in-apache-flink-5c4m)
✅ Code examples can be found [here](https://github.com/polyzos/flink-blogpost-examples/tree/main/src/main/java/io/ipolyzos/triggers).

-----------------------
### Deploy a JAR file
You can run the examples directly from your you IDE like IntelliJ IDEA. However, if you want to deploy the jar file to a Flink cluster, you can follow the steps below.

1. Package the application and create an executable jar file
```shell
mvn clan package
```

2. Run the flink job
```shell
./bin/flink run \
  --class io.ipolyzos.<your-main-class> \
  ./flink-blogpost-examples-0.1.0.jar
```