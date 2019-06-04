Fusion
======

Fusion is a highly extensible data processing framework to enable users to quickly create 
Apache Spark applications, such as an extract, transform, and load (ETL)
application. Fusion has various readers to extract data from many sources
and convert them to Spark DataFramee for further processing. Fusion also has
various writer to load Spark DataFrame into data store, such as data lake or
Apache Kafka. Fusion also acts as the task orchestrator to resolve task
dependencies and use an internal variable system to allow data exchange among
tasks.

## Table of Contents ##

- [Installation](#installation)
- [Quick Start](#quick-start)
- [Use Fusion](#use-fusion)
- [Task Processors](#task-processors)
- [Advanced Topics](#advanced-topics)
- [Management UI](#management-ui)
- [Examples](#examples)

## Installation

#### Software Requreiments
1. JDK 8 (Supports OpenJDK and Oracle JDK)
2. Apache Spark 2.3 / 2. 4
3. winutils If runs on Microsoft Windows


## Quick Start
1. Set environment variable SPARK_HOME if spark-submit is not in the PATH
2. (On Linux/Mac) `tar -xzvf fusion-0.2.3-bin.tar.gz ` 
(On Windows), unzip fusion-0.2.3-bin.zip
3. Run `bin/fusion-sandbox.sh` on Linux/Mac or `bin\fusion-sandbox.cmd` on Windows
4. On web browser, open the link http://localhost:14080/ 


## Use Fusion
Fusion uses YAML format to store configuration. The default name of the main configuration is `Fusion.yaml`.
Use can specify a different yaml file in the command line to start Fusion.  
We can add one or more sections to the YAML file. Each section represents a task.
The key of a section is the task name and the value of the section is the 
A task can be "starting Spark"

## Task Processors

#### spark.Start


## Advanced Topics


## Management UI


## Examples

