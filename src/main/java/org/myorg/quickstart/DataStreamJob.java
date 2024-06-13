/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.myorg.quickstart;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.jdbc.catalog.JdbcCatalog;
import org.apache.flink.connector.jdbc.table.JdbcConnectorOptions;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.table.api.*;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.functions.TableFunction;

import java.util.Arrays;

import static org.apache.flink.table.api.Expressions.*;

/**
 * Skeleton for a Flink DataStream Job.
 *
 * <p>For a tutorial how to write a Flink application, check the
 * tutorials and examples on the <a href="https://flink.apache.org">Flink Website</a>.
 *
 * <p>To package your application into a JAR file for execution, run
 * 'mvn clean package' on the command line.
 *
 * <p>If you change the name of the main class (with the public static void main(String[] args))
 * method, change the respective entry in the POM.xml file (simply search for 'mainClass').
 */
public class DataStreamJob {

    public static void main(String[] args) throws Exception {

       /* StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<Tuple2<String, Integer>> dataStream = env
                .socketTextStream("localhost", 9999)
                .flatMap(new Splitter())
                .keyBy(value -> value.f0)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                .sum(1);
        dataStream.print();
        env.execute("Window WordCount");*/

        // Sets up the execution environment, which is the main entry point
        // to building Flink applications.
        // StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

          /* EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                //.inStreamingMode()
                .inBatchMode()
                .build();
        TableEnvironment tableEnv = TableEnvironment.create(settings);
        // Streaming runtime mode, on the other hand, can be used for both bounded and unbounded jobs.
        // set the batch runtime mode
        // env.setRuntimeMode(RuntimeExecutionMode.BATCH);*/

        // variables - can move to a yaml
        String name = "mycatalog";
        String defaultDatabase = "employees";
        String username = "keith";
        String password = "Password1";
        String baseUrl = "jdbc:mysql://localhost:3306";
        String mongodb = "mongodb://localhost:27017";
        String connector = "mongodb";
        String mongoAppDb = "flinkdb";
        String collection = "employees";

        // default catalog MUST CREATE TABLES IN MYSQL MANUALLY OR USE A JDBC HELPER FIRS
        /**
        String sqlCreateEmp = "CREATE TABLE IF NOT EXISTS employees " +
                "(emp_no INT, birth_date DATE, first_name STRING, last_name STRING," +
                "gender STRING, hire_date DATE, PRIMARY KEY (emp_no) NOT ENFORCED) " +
                "WITH " +
                "('connector' = 'jdbc'," +
                "'driver' = 'com.mysql.cj.jdbc.Driver'," +
                "'url' = 'jdbc:mysql://localhost:3306/employees'," +
                "'username' = 'keith', " +
                "'password' = 'Password1', " +
                "'table-name' = 'employees');";
        tableEnv.executeSql(sqlCreateEmp);

        Table tableResultEmp = tableEnv.sqlQuery("SELECT * FROM employees");
        tableResultEmp .execute().print();
        **/

        /*
        // create table with kafka topic
        tableEnv.executeSql("CREATE TABLE employees-topic " +
                " ( emp_no INT, " +
                " birth_date DATE, " +
                " first_name STRING, " +
                " last_name STRING, " +
                " gender STRING, " +
                " hire_date DATE" +
                //"WATERMARK FOR transaction_time AS transaction_time - INTERVAL '5' SECOND\n" +
                ") WITH (" +
                " 'connector' = 'kafka'," +
                " 'topic' = 'employees-topic'," +
                " 'properties.bootstrap.servers' = 'localhost:9092'," +
                " 'scan.startup.mode' = 'earliest-offset'," +
                " 'format' = 'csv'" +
                ")");
            *//*
            kafka-console-producer \
            --topic employees-topic \
            --bootstrap-server localhost:9092 \
            < employees2.csv
            *//*
        Table kafkaEmployees = tableEnv.from("employees-topic").select($("*"));*/

        // create mysql table in flink
       /* String sqlCreateEmp2 = "CREATE TABLE IF NOT EXISTS employees2 " +
                " (emp_no INT, birth_date DATE, first_name STRING, " +
                " last_name STRING, gender STRING, hire_date DATE) " +
                " WITH ('connector' = 'jdbc', " +
                " 'driver' = 'com.mysql.cj.jdbc.Driver'," +
                " 'url' = 'jdbc:mysql://localhost:3306/employees', " +
                " 'username' = 'keith'," +
                " 'password' = 'Password1', " +
                " 'table-name' = 'employees2');";
        tableEnv.executeSql(sqlCreateEmp2);*/

        // load table in mysql from kafka
//        String sqlLoadEmployeesTable = " INSERT INTO employees2 " +
//                " SELECT emp_no, " +
//                " birth_date," +
//                " first_name," +
//                " last_name," +
//                " gender," +
//                " hire_date" +
//                " FROM employees-topic;";
//        TableResult tableResultEmp2 = tableEnv.executeSql(sqlLoadEmployeesTable);
        //*** or Table employees2 = kafkaEmployees.select($"*");

        // create mongodb collection in flink
        /*String sqlCreateEmpMongo = "CREATE TABLE IF NOT EXISTS employees3 " +
                " (emp_no INT, " +
                " birth_date TIMESTAMP_LTZ(3), " +
                " first_name STRING, " +
                " last_name STRING, " +
                " gender STRING, " +
                " hire_date TIMESTAMP_LTZ(3)" +
                " ) WITH (" +
                " 'connector' = 'mongodb', " +
                "'uri' = 'mongodb://keith:Password1@localhost:27017', " +
                "'database' = 'flinkdb', " +
                "'collection' = 'employees3');";
        tableEnv.executeSql(sqlCreateEmpMongo);
        // insert into mongo --> this can be changed to a pipeline i.e. result.insert
        String insertStmtMongo = "INSERT INTO employees3 " +
                "SELECT emp_no, " +
                "cast(birth_date as TIMESTAMP_LTZ(3)) as birth_date, " +
                "first_name, " +
                "last_name, " +
                "gender, " +
                "cast(hire_date as TIMESTAMP_LTZ(3)) as hire_date " +
                "FROM employees2";
        TableResult tableResultMongo = tableEnv.executeSql(insertStmtMongo);
        // tableResultMongo.execute().print();
        System.out.println(tableResultMongo.getJobClient().get().getJobStatus());*/

        // table function
        /*TableFunction func = new DataStreamFlatMap();
        tableEnv.registerFunction("func", func);

        Table employees2 = tableEnv.from("employees2").select($("*"));;
        Table tableEmpNo = employees2.flatMap(call("func", $("*")));
        tableEmpNo.execute().print();*/
        //Table tableEmpno = employees2.map(call("func"));
        //# specify the function without the input columns
        //table.map(func2).execute().print();

        // Table input = employees2.select($("*"));
        //Table table = input.map(call("func", $("emp_no")));
        //Table table = input.select($("emp_no"));
        //table.execute().print();*/

        /******************************************************************************
         // execute SELECT statement
         TableResult tableResult1 = tableEnv.executeSql("SELECT * FROM Orders");
         // use try-with-resources statement to make sure the iterator will be closed automatically
         try (CloseableIterator<Row> it = tableResult1.collect()) {
         while(it.hasNext()) {
         Row row = it.next();
         // handle row
         }
         }
         //
         // execute Table
         TableResult tableResult2 = tableEnv.sqlQuery("SELECT * FROM Orders").execute();
         tableResult2.print();
         ******************************************************************************/

        // JDBC CATALOG
        /*JdbcCatalog catalog = new JdbcCatalog(name, defaultDatabase, username, password, baseUrl);
        tableEnv.registerCatalog("mycatalog", catalog);
        tableEnv.useCatalog("mycatalog");*/

        //using jdbc catalog- tables are registered by default
        /*Table employees = tableEnv.from("employees");
        Table employeesDetails = employees.select($("*"));
        employeesDetails.execute().print();*/

        //using jdbc catalog tables are registered by default
        /*String insertStmt = "INSERT INTO mycatalog.employees.employees2 SELECT * FROM mycatalog.employees.employees";
        TableResult result = tableEnv.executeSql(insertStmt);
        //System.out.println(result.getJobClient().get().getJobStatus())*/

        //mongodb
        // tableEnv.useCatalog("default_catalog");
        // tableEnv.useDatabase("default_database");


        /*tableEnv.executeSql("CREATE TABLE employees " +
                 "(emp_no INT, " +
                "birth_date DATE, " +
                "first_name STRING, " +
                "last_name STRING, " +
                "gender STRING, " +
                "hire_date DATE" +
                //"WATERMARK FOR transaction_time AS transaction_time - INTERVAL '5' SECOND\n" +
                ") WITH (" +
                "    'connector' = 'kafka',\n" +
                "    'topic'     = 'employees',\n" +
                "    'properties.bootstrap.servers' = 'localhost:9092',\n" +
                "    'scan.startup.mode' = 'earliest-offset',\n" +
                "    'format'    = 'csv'\n" +
                ")");

        Table kafkaEmployees = tableEnv.from("employees").select($("*"));
        kafkaEmployees.execute().print();*/




        /*run multiple INSERT queries on the registered source table and emit the result to registered sink tables
        //StatementSet stmtSet = tEnv.createStatementSet();
        //stmtSet.addInsertSql()
        //TableResult tableResult2 = stmtSet.execute();
        //System.out.println(tableResult2.getJobClient().get().getJobStatus());
        */


        /* table pipeline method defines a complete end-to-end pipeline emitting the source table to a registered sink table.
        // compute a result Table using Table API operators and/or SQL queries
        Table result = ...;
        // Prepare the insert into pipeline
        TablePipeline pipeline = result.insertInto("CsvSinkTable");
        // Print explain details
        pipeline.printExplain();
        // emit the result Table to the registered TableSink
        pipeline.execute();
        */

        /*
        tEnv.executeSql("CREATE TABLE employees (\n" +
                "    account_id  BIGINT,\n" +
                "    amount      BIGINT,\n" +
                "    transaction_time TIMESTAMP(3),\n" +

                "    WATERMARK FOR transaction_time AS transaction_time - INTERVAL '5' SECOND\n" +
                ") WITH (\n" +
                "    'connector' = 'kafka',\n" +
                "    'topic'     = 'employees',\n" +
                "    'properties.bootstrap.servers' = 'localhost:9092',\n" +
                "    'scan.startup.mode' = 'earliest-offset',\n" +
                "    'format'    = 'csv'\n" +
                ")");*/

        //Arrays.stream(tableEnv.listTables()).sorted().forEach(System.out::println);
        // drop table
        //catalog.dropTable(new ObjectPath("mycatalog.employees", "employees2"), false);
        //TableResult tableResult1 =  tableEnv.executeSql(sqlCreateEmp);
        //System.out.println(tableResult1.getJobClient().get().getJobStatus());*/
        //JdbcCatalog catalog = new JdbcCatalog(name, defaultDatabase, username, password, baseUrl);
        //tableEnv.registerCatalog("mycatalog", catalog);
        //tableEnv.useCatalog("mycatalog");

     /*  final Schema schema = Schema.newBuilder()
                .column("emp_no", DataTypes.INT())
                .column("birth_date", DataTypes.DATE())
                .column("first_name", DataTypes.STRING())
                .column("last_name", DataTypes.STRING())
                .column("gender", DataTypes.STRING())
                .column("hire_date", DataTypes.DATE())
                .build();

        final TableDescriptor sourceDescriptor = TableDescriptor.forConnector("jdbc")
                .schema(schema)
                .option(JdbcConnectorOptions.DRIVER, "com.mysql.cj.jdbc.Driver")
                .option(JdbcConnectorOptions.URL, "jdbc:mysql://localhost:3306/employees")
                .option(JdbcConnectorOptions.USERNAME, "keith")
                .option(JdbcConnectorOptions.PASSWORD, "Password1")
                .option(JdbcConnectorOptions.TABLE_NAME, "employees")
                .build();*/


        //register
        //tableEnv.createTable("employees", sourceDescriptor);

        /*Table employees = tableEnv.from(sourceDescriptor);
        Table result = employees.select($("*"));
        result.execute().print();*/

        //mysql
        /*final TableDescriptor destDescriptor = TableDescriptor.forConnector("jdbc")
                .schema(schema)
                .option(JdbcConnectorOptions.DRIVER, "com.mysql.cj.jdbc.Driver")
                .option(JdbcConnectorOptions.URL, "jdbc:mysql://localhost:3306/employees")
                .option(JdbcConnectorOptions.USERNAME, "keith")
                .option(JdbcConnectorOptions.PASSWORD, "Password1")
                .option(JdbcConnectorOptions.TABLE_NAME, "details")
                .option(JdbcConnectorOptions.SINK_BUFFER_FLUSH_INTERVAL, Duration.ofSeconds(1L))
                .option(JdbcConnectorOptions.SINK_BUFFER_FLUSH_MAX_ROWS, 1000)
                .option(JdbcConnectorOptions.SINK_MAX_RETRIES, 3)
                .build();*/

        //kafka
         /*TableEnvironment tEnv = TableEnvironment.create(settings);
        tEnv.executeSql("CREATE TABLE transactions (\n" +
                "    account_id  BIGINT,\n" +
                "    amount      BIGINT,\n" +
                "    transaction_time TIMESTAMP(3),\n" +
                "    WATERMARK FOR transaction_time AS transaction_time - INTERVAL '5' SECOND\n" +
                ") WITH (\n" +
                "    'connector' = 'kafka',\n" +
                "    'topic'     = 'transactions',\n" +
                "    'properties.bootstrap.servers' = 'localhost:9092',\n" +
                "    'scan.startup.mode' = 'earliest-offset',\n" +
                "    'format'    = 'csv'\n" +
                ")");*/

        /* Kafka DataSream */
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //Kafka Source
        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers("localhost:9092")
                .setTopics("my-topic")
                .setGroupId("my-group-id")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();
        //kafkaSource.builder().setTopics("topic-a", "topic-b")
        //KafkaSource.builder().setTopicPattern("topic.*");

        //final HashSet<TopicPartition> partitionSet = new HashSet<>(Arrays.asList(
        //new TopicPartition("topic-a", 0),    // Partition 0 of topic "topic-a"
        //new TopicPartition("topic-b", 5)));  // Partition 5 of topic "topic-b"
        // KafkaSource.builder().setPartitions(partitionSet);
        // deprecated - FlinkKafkaConsumer is deprecated

        DataStream<String> stream = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source");
        //DataStream<Tuple2<String, Integer>> processedStream = stream
        //stream.print();
        //env.execute();
      
      
      
      /* DataStream<Tuple2<String, Integer>> wordCounts = env.fromElements(
                new Tuple2<String, Integer>("hello", 1),
                new Tuple2<String, Integer>("world", 2));

        wordCounts.map(new MapFunction<Tuple2<String, Integer>, Integer>() {
            @Override
            public Integer map(Tuple2<String, Integer> value) throws Exception {
                return value.f1;
            }
        });

        wordCounts.keyBy(value -> value.f0);
        */

        DataStream<String> control = env
        .fromElements("DROP", "IGNORE")
        .keyBy(x -> x);

    DataStream<String> streamOfWords = env
        .fromElements("Apache", "DROP", "Flink", "IGNORE")
        .keyBy(x -> x);
  
    control
        .connect(streamOfWords)
        .flatMap(new ControlFunction())
        .print();

    env.execute();


    }
}