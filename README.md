Fusion
======

Fusion is a highly extensible data processing framework to enable users to quickly create 
Apache Spark applications, such as an extract, transform, and load (ETL)
application. Fusion has various readers to extract data from many sources
and convert them to Spark DataFrame for further processing. Fusion also has
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

#### Software Requirements
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
Users can specify a different yaml file in the command line to start Fusion.  

In the configuration, we can add one or more root level nodes to define tasks. 
The node name is the name of the task. 
Each task is processed by a task processor. There are many pre-built task processors. 
Alternatively, users can extend the com.fuseinfo.fusion.FusionFunction class to create new task processor.
By default, Fusion will execute the tasks one after another upon completion. 
We can also set a task to run in background mode.
So, the next task would be started immediately without waiting for the completion of the previous task.
To enable background mode, we can append an & at the end of the task name.
 
If we are not going to pass any parameter to a task processor, 
we can directly set the name of the task processor or the class name of the task processor a the String value of the root level node.
If we want to provide one or more parameters to the task processor, we can make the node as a Map.
In the Map node, we need to provide an attribute `__class` with the value as the name or Java class of the task processor.
Optionally, we may provide an attribute `__deps` with the value as a list of previous task names, separated by comma.
In this case, Fusion will make sure all these tasks were completed successfully before starting this task.
Other attributes would be provided to the task processor as parameters.


## Task Processors

### List of Processors
Parameters (mandatory in **bold**):  

#### spark.Start
Use this processor to start SparkSession.  
All parameters starts with `spark.` will be provided to SparkConf. 
All parameters starts with `hadoop.` will be provided to HadoopConf with the prefix `hadoop.` being stripped out.

#### spark.Jetty
use this processor to start Web Management UI
- keyStore: Provide a keyStore and keyStorePassword to enable SSL connection.
- keyStorePassword: Provide a keyStore and keyStorePassword to enable SSL connection.
- trustStore: Java trust store
- login: authentication type, either ldap or file. If this value is not provided, no authentication is required

#### spark.reader.AvroReader
- **path**: The directory of the Avro files
- repartition: Number of partitions
- cache: Set to true to cache the DataFrame. We can also set the value to a [Storage Level](https://spark.apache.org/docs/2.3.0/rdd-programming-guide.html#rdd-persistence) 

#### spark.reader.CobolReader
- **path**: The directory of the COBOL files
- **copybook**: The COBOL copybook that describe the files
- bookname: The common prefix of field names to be ignored
- recordformat: Set to F or FB if the record length of the files is fixed. Set to V or VB if the record length is variable.
- binaryformat: Type of binary format. 0 - Intel; 1 - Mainframe; 2 - Fujitsu; 3 - Big-Endian; 4 - GNU COBOL; Default is 1
- font: codepage. If the binaryformat is 1, the default font is cp037
- copybookformat: Default value is 1, which means to use standard column 7 to 72 
- emptyvalue: use this string when empty is found
- nullvalue: use this string when null is found
- number: How to format a number. Valid values are: decimal, double, string, mixed. Default is `mixed`.
- split: How to split the value. Default value is 0 - none
- tree: Whether we use tree structure to describe the fields. Default value is false
- repartition: Number of partitions
- cache: Set to true to cache the DataFrame. We can also set the value to a [Storage Level](https://spark.apache.org/docs/2.3.0/rdd-programming-guide.html#rdd-persistence) 

#### spark.reader.CsvReader
- **path**: The directory of the COBOL files
- delimiter: The delimiter of the files. The default value is `,`
- quote: The quote character of the files. The default value is `"`
- escape: The escape character for special character in quoted strings. The default value is \
- charset: The character set of the files. The default value is UTF-8
- header: Whether the file contains a header line to describe the name of the fields. The default value is false
- skipLines: The number of lines to skip.
- multiLine: To enable multi-line mode. The default value is false.
- inferSchema: Whether to infer the schema
- schema: To specify an Avro schema of the files
- fields: list of field names.
- dateFormat: dateFormat to cast values to Date type
- timeZone: The default timezone
- timestampFormat: dateFormat to cast values to Timestamp type
- maxColumns: maximum number of columns
- maxCharsPerColumn: maximum length of a column
- comment: Leading character to indicating the line to be ignored as comments
- charToEscapeQuoteEscaping:
- ignoreLeadingWhiteSpace: Whether to ignore leading white spaces.
- ignoreTrailingWhiteSpace: Whether to ignore trailing white spaces.
- columnNameOfCorruptRecord: The column name to store corrupted records
- repartition: Number of partitions
- cache: Set to true to cache the DataFrame. We can also set the value to a [Storage Level](https://spark.apache.org/docs/2.3.0/rdd-programming-guide.html#rdd-persistence) 

#### spark.reader.ExcelReader
- **path**: The directory of the Excel files
- sheet: The name or sequence number of the worksheet to read. The default behavior is to read the first worksheet.
- header: Whether the file contains a header line to describe the name of the fields. The defautl value is false.
- skipLines: The number of lines to skip.
- inferSchema: Whether to infer the schema
- fields: list of field names.
- emptyValue: Default empty value
- dateFormat: dateFormat to cast values to Date type
- timestampFormat: dateFormat to cast values to Timestamp type
- repartition: Number of partitions
- cache: Set to true to cache the DataFrame. We can also set the value to a [Storage Level](https://spark.apache.org/docs/2.3.0/rdd-programming-guide.html#rdd-persistence) 

#### spark.reader.FixedLengthReader
- **path**: The directory of the Flat fixed length files
- **fields**: A list of fields, separated by ,
- schema: To specify an Avro schema of the files
- dateFormat: dateFormat to cast values to Date type
- timestampFormat: dateFormat to cast values to Timestamp type
- repartition: Number of partitions
- cache: Set to true to cache the DataFrame. We can also set the value to a [Storage Level](https://spark.apache.org/docs/2.3.0/rdd-programming-guide.html#rdd-persistence)     

#### spark.reader.JdbcReader
- **url**: JDBC url of the database
- **driver**: JDBC driver class of the database
- **table**: The table or SQL query to be loaded
- user: The JDBC user
- password: The password of the JDBC user
- where: Add a WHERE clause to the table/SQL query
- partitionColumn: Provide a numeric column/expression to partition of JDBC connection
- lowerBound: The lower Bound value of the partition column
- upperBound: The upper Bound value of the partition column
- numPartitions: The number of partitions
- fetchsize: Number of record to be fetch at a time
- repartition: Number of partitions
- cache: Set to true to cache the DataFrame. We can also set the value to a [Storage Level](https://spark.apache.org/docs/2.3.0/rdd-programming-guide.html#rdd-persistence)     

#### spark.reader.KafkaReader
- **topic**: Kafka topic
- **kafka.bootstrap.servers**: Kafka bootstrap servers
- **kafka.group.id**: Kafka group id
- kafka.schema.registry.url: Schema registry url
- schema: To specify an Avro schema of the files
- ranges: List of offset range per partition to be loaded
- offsets: List of beginning offset per partition
- repartition: Number of partitions
- cache: Set to true to cache the DataFrame. We can also set the value to a [Storage Level](https://spark.apache.org/docs/2.3.0/rdd-programming-guide.html#rdd-persistence)     

#### spark.reader.OrcReader
- **path**: The directory of the ORC files
- repartition: Number of partitions
- cache: Set to true to cache the DataFrame. We can also set the value to a [Storage Level](https://spark.apache.org/docs/2.3.0/rdd-programming-guide.html#rdd-persistence) 

#### spark.reader.ParquetReader
- **path**: The directory of the Parquet files
- repartition: Number of partitions
- cache: Set to true to cache the DataFrame. We can also set the value to a [Storage Level](https://spark.apache.org/docs/2.3.0/rdd-programming-guide.html#rdd-persistence) 

#### spark.writer.AvroWriter
- **path**: The directory of the output Avro files
- sql: An SQL query to construct the output. We can provide either `sql` or `table`.
- table: DataFrame to be persisted. We can provide either `sql` or `table`.
- partitionBy:  A list of partition columns, separated by ,
- verifyCounts: Whether to verify record counts between DataFrame and persisted files. Default value is false
- user: Run as a different user
- keytab: keytab of the user
- filePrefix: prefix of the output file name
- staging: staging location.
- coalesce: Number of partitions to coalesce
- repartition: Number of partitions to repartition
- onSuccess: A list of extensions upon successful execution
- onFailure: A list of extensions upon unsuccessful execution

#### spark.writer.CsvWriter
- **path**: The directory of the output CSV files
- sql: An SQL query to construct the output. We can provide either `sql` or `table`.
- table: DataFrame to be persisted. We can provide either `sql` or `table`.
- header: Whether to add a header to csv files
- delimiter: delimiter character. Default value is ,
- quote: quote character. Default value is "
- escape: escape character. Default value is \
- nullValue: String to describe null value
- dateFormat: DateFormat to format Date to String
- partitionBy:  A list of partition columns, separated by ,
- verifyCounts: Whether to verify record counts between DataFrame and persisted files. Default value is false
- user: Run as a different user
- keytab: keytab of the user
- filePrefix: prefix of the output file name
- staging: staging location.
- coalesce: Number of partitions to coalesce
- repartition: Number of partitions to repartition
- onSuccess: A list of extensions upon successful execution
- onFailure: A list of extensions upon unsuccessful execution
    
#### spark.writer.KafkaWriter
- **topic**: Kafka topic to be written
- **kafka.bootstrap.servers**: Kafka bootstrap servers
- kafka.schema.registry.url: Kafka schema registry url"
- sql: An SQL query to construct the output. We can provide either `sql` or `table`.
- table: DataFrame to be persisted. We can provide either `sql` or `table`.
- keyColumn: To construct the key portion of the message using a list of columns
- coalesce: Number of partitions to coalesce
- repartition: Number of partitions to repartition
- onSuccess: A list of extensions upon successful execution
- onFailure: A list of extensions upon unsuccessful execution

#### spark.writer.OrcWriter
- **path**: The directory of the output Orc files
- sql: An SQL query to construct the output. We can provide either `sql` or `table`.
- table: DataFrame to be persisted. We can provide either `sql` or `table`.
- partitionBy:  A list of partition columns, separated by ,
- verifyCounts: Whether to verify record counts between DataFrame and persisted files. Default value is false
- user: Run as a different user
- keytab: keytab of the user
- filePrefix: prefix of the output file name
- staging: staging location.
- coalesce: Number of partitions to coalesce
- repartition: Number of partitions to repartition
- onSuccess: A list of extensions upon successful execution
- onFailure: A list of extensions upon unsuccessful execution

#### spark.writer.ParquetWriter
- **path**: The directory of the output Parquet files
- sql: An SQL query to construct the output. We can provide either `sql` or `table`.
- table: DataFrame to be persisted. We can provide either `sql` or `table`.
- partitionBy:  A list of partition columns, separated by ,
- verifyCounts: Whether to verify record counts between DataFrame and persisted files. Default value is false
- user: Run as a different user
- keytab: keytab of the user
- filePrefix: prefix of the output file name
- staging: staging location.
- coalesce: Number of partitions to coalesce
- repartition: Number of partitions to repartition
- onSuccess: A list of extensions upon successful execution
- onFailure: A list of extensions upon unsuccessful execution

#### spark.SQL
- **sql**: An SQL query to transform
- repartition: Number of partitions
- cache: Set to true to cache the DataFrame. We can also set the value to a [Storage Level](https://spark.apache.org/docs/2.3.0/rdd-programming-guide.html#rdd-persistence) 

#### spark.HadoopCopy
- **source**: Source directory
- **target**: Target directory
- pattern: Regular expression of the source files to ingest
- recursive: Whether scanning files Recursively
- digest: Digest method
- beforeCopy: Function before copy
- afterCopy: Function after copy
- compressCodec: Compress codec
- sourceUser: Alternative user for reading
- sourceKeytab: Keytab for the source user
- targetUser: Alternative user for writing
- targetKeytab: Keytab for the target user
- interval: checking interval in ms
- wait: delay before copying in ms
- staging: Temporary staging folder

#### spark.ScalaFunction
- **scala**: scala code to run. val `spark:SparkSession` is initialized as


## Advanced Topics
#### Variables and Commands
We can use variables to pass information from the runtime environment to the application or from task to task.
We can define a variable in the Fusion command line by setting *var_name*=*value*  
In addition, all system environment variables can be referred as variable in Fusion.  
To use a variable, we can use the syntax to ${*varName*} in the configuration.
We can also provide a default value when the variable is not initialized as ${*varName*:-*defaultValue*}  
For example, we can set `${env:-prod}`.
In this case Fusion will try to retrieve if a variable called `env` is defined.
If it is defined the string will be replaced by the value of the variable. Otherwise, the value will be `prod`.

We may also run a command function and the output of the command function will be returned.
To run a Command, we can use the syntax $(*Command* *Parameter*)  
Command and variables can be nested. In another words, we can use variables as a parameter to a command, 
or use a command to form a default value of a variable.


#### Extensions
Some processors would persist output data. We can define one or more extension functions upon success or failure.
An extension function is an implementation of `Map[String, String] => Boolean`.
We can use extension to report lineage, statistics, as well as error handling. 

## Management UI


## Examples

