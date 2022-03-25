package com.mobin

import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.internal.{TableEnvironmentImpl, TableEnvironmentInternal}
import org.apache.flink.table.delegation.Parser
import org.junit.{After, Before, Test}

class MlinkTest {

  var tabEnvInternal: TableEnvironmentInternal = _

  var parser: Parser = _

  var mlinkClient: MlinkClient = _

  var sql:String = _

  @Before
  def setup(): Unit = {
    val settings =
      EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build()
    val tableEnv = TableEnvironmentImpl.create(settings)
    tabEnvInternal = tableEnv.asInstanceOf[TableEnvironmentInternal]
    parser = tableEnv.asInstanceOf[TableEnvironmentInternal].getParser
    mlinkClient = new MlinkClient()
  }

  @After
  def execute(): Unit ={
    val result = mlinkClient.executeInitialization(sql)
    if (result != null) {
      result.getJobClient.get().getJobExecutionResult.get()
    }
  }



  @Test
  def mysqlConnectFailed(): Unit = {
    //可测试链接mysql失败的场景
    sql =
      """
        |SET 'execution.checkpointing.interval' = '1min';
        |SET 'execution.checkpointing.min-pause' = '10s';
        |
        |CREATE TABLE source_table (
        |    user_id INT,
        |    cost DOUBLE,
        |    ts AS localtimestamp,
        |    procTime AS PROCTIME()
        |) WITH (
        |      'connector' = 'datagen',
        |      'rows-per-second'='1',
        |      'fields.user_id.kind'='random',
        |      'fields.user_id.min'='1',
        |      'fields.user_id.max'='10',
        |      'fields.cost.kind'='random',
        |      'fields.cost.min'='1',
        |      'fields.cost.max'='100'
        |      );
        |
        |CREATE TABLE mysqltest (
        |    user_id           INT,
        |    cost              DOUBLE,
        |    PRIMARY KEY (user_id) NOT ENFORCED
        |) WITH (
        |      'connector' = 'jdbc',
        |      'username' = 'root',
        |      'password' = 'sjMobinee',
        |      'url' = 'jdbc:mysql://localhost:3306/mobin?socketTimeout=100000&characterEncoding=UTF-8&autoReconnect=true&allowMultiQueries=true&serverTimezone=UTC',
        |      'table-name' = 'mysqltest'
        |      );
        |
        | INSERT INTO mysqltest
        |SELECT
        |    user_id,
        |    cost
        |FROM source_table
        |GROUP BY user_id,cost,TUMBLE(procTime, INTERVAL '5' SECOND );
        |
        |
        |""".stripMargin
  }

  @Test
  def icebergToInsertOneData(): Unit = {
    sql =
      """
        |SET execution.runtime-mode = BATCH;
        |--SET 'execution.checkpointing.interval' = '1min';
        |--SET 'execution.checkpointing.min-pause' = '10s';
        |SET 'parallelism.default' = '3';
        |
        | CREATE TABLE iceberg_sbtest_part3 (
        |                                 id int,
        |                                 name STRING,
        |                                 PRIMARY KEY (id) NOT ENFORCED
        |)
        |--PARTITIONED BY(stat_date,stat_hour)
        |WITH (
        |      'connector'='iceberg',
        |      'format-version' = '2',
        |      'catalog-name'='hadoop_prod',  --rrgfg
        |      'catalog-type'='hadoop',
        |           'write.upsert.enabled'='true',
        |--       'write.distribution-mode'='hash',
        |      'warehouse'='file:///tmp/flink/iceberg/'
        |      );
        |
        |INSERT INTO iceberg_sbtest_part3 VALUES(1,'aa');
        |
        |
        |""".stripMargin
  }

  @Test
  def icebergUpsert(): Unit = {
    sql =
      """
        |
        |CREATE TABLE iceberg_upsert_test_mysql(
        |   id INT,
        |   name STRING,
        |   age INT,
        |   PRIMARY KEY(id) NOT ENFORCED
        |) WITH (
        |      'connector' = 'mysql-cdc',
        |      'hostname' = 'localhost',
        |      'port' = '3306',
        |      'username' = 'root',
        |      'password' = 'sjMobin',
        |      'database-name' = 'mobin',
        |      'table-name' = 'iceberg_upsert_test'
        |      );
        |
        |CREATE TABLE iceberg_upsert_test (
        |                                 id INT,
        |                                 name STRING,
        |                                 age INT
        |) WITH (
        |      'connector'='print'
        |     -- 'format-version' = '2',
        |     -- 'catalog-name'='hadoop_prod',
        |     -- 'catalog-type'='hadoop',
        |     --  'write.upsert.enabled'='true',
        |     --  'write.distribution-mode'='hash',
        |     -- 'warehouse'='file:///tmp/flink/iceberg/'
        |      );
        |
        |INSERT INTO iceberg_upsert_test SELECT * FROM iceberg_upsert_test_mysql;
        |
        |""".stripMargin
  }

  @Test
  def mysqlCDCToKafka(): Unit = {
    sql =
      """
        |
        |CREATE TABLE test_prm5_cdc(
        |   id INT,
        |   name STRING,
        |   age INT,
        |   PRIMARY KEY(id) NOT ENFORCED
        |) WITH (
        |      'connector' = 'mysql-cdc',
        |      'hostname' = 'localhost',
        |      'port' = '3306',
        |      'username' = 'root',
        |      'password' = 'sjMobin',
        |      'database-name' = 'mobin',
        |      'table-name' = 'iceberg_upsert_test'
        |      );
        |
        |CREATE TABLE mysql_cdc_kafka(
        |   id INT,
        |   name STRING,
        |   age INT,
        |   procTime AS PROCTIME(),
        |   PRIMARY KEY (id) NOT ENFORCED
        |) WITH (
        |      'connector' = 'kafka',
        |      'key.fields'='id',
        |      'key.format'='json',
        |      'topic' = 'mysql-cdc-to-iceberg3',
        |      'properties.group.id' = 'Flink-test.kafka-iceberg',
        |      'properties.bootstrap.servers' = 'localhost:9092',
        |      'format' = 'changelog-json'
        |      );
        |
        |INSERT INTO mysql_cdc_kafka SELECT * FROM test_prm5_cdc;
        |
        |""".stripMargin
  }

  @Test
  def changeJSONKafkaToIceberg(): Unit = {
    sql =
      """
        |
        |CREATE TABLE mysql_cdc_kafka(
        |   id INT,
        |   name STRING,
        |   age INT,
        |   procTime AS PROCTIME(),
        |   PRIMARY KEY (id) NOT ENFORCED
        |) WITH (
        |      'connector' = 'kafka',
        |      'key.fields'='id',
        |      'key.format'='json',
        |      'scan.startup.mode'='earliest-offset',
        |      'topic' = 'mysql-cdc-to-iceberg3',
        |      'properties.group.id' = 'Flink-test.kafka-iceberg',
        |      'properties.bootstrap.servers' = 'localhost:9092',
        |      'format' = 'changelog-json'
        |      );
        |
        |CREATE TABLE iceberg_upsert_test (
        |                                 id INT,
        |                                 name STRING,
        |                                 age INT,
        |                                 PRIMARY KEY (id) NOT ENFORCED
        |) WITH (
        |      'connector'='iceberg',
        |      'format-version' = '2',
        |      'catalog-name'='hadoop_prod',
        |      'catalog-type'='hadoop',
        |       'write.upsert.enabled'='true',
        |       'write.distribution-mode'='hash',
        |      'warehouse'='file:///tmp/flink/iceberg/'
        |      );
        |
        |INSERT INTO iceberg_upsert_test SELECT id,name,age FROM mysql_cdc_kafka;
        |
        |""".stripMargin
  }

  @Test
  def unsetAarrayJson: Unit ={
    //json数组中包含嵌套json
    sql =
      """
        |CREATE TABLE test_log(
        |  logContents ARRAY<ROW<
        |        fc_distinct_id string,
        |        fc_time bigint,
        |        fc_biz_type string,
        |     --   fc_biz_content string,
        |        fc_biz_content ROW<orderId STRING, playTimes int, playDuration int>
        |--        orderId string,
        |--        playDuration int,
        |--        playTimes int
        |    >>,
        |    PROCTIME AS PROCTIME()
        |) WITH (
        |    'connector' = 'kafka',
        |    'topic' = 'bizReportFrontEndLog',
        |    'format' = 'mobin-json',
        |    'mobin-json.encrypt' = 'true',
        |    'mobin-json.ignore-parse-errors' = 'true',
        |    'scan.startup.mode' = 'earliest-offset',
        |--    'scan.startup.mode' = 'group-offsets',
        |    'properties.group.id' = 'test2',
        |    'properties.bootstrap.servers' = 'localhost:9092'  -- sit3
        |);
        |
        |CREATE TABLE test_log_print(
        |  orderId String
        |) WITH(
        |   'connector' = 'print'
        |);
        |
        |INSERT INTO test_log_print SELECT orderId FROM test_log,UNNEST(test_log.logContents) t(fc_distinct_id,fc_time,fc_biz_type,fc_biz_content);
        |
        |""".stripMargin
  }

  @Test
  def redisSet(): Unit = {
    sql =
      """
        |
        |CREATE TABLE redistest (
        |    id STRING,
        |    age STRING
        |) WITH (
        | 'connector' = 'redis',
        | 'host'='localhost',
        | 'port' = '8080',
        | 'command'='SET',
        | 'password'='sjmobin',
        | 'mode'='cluster'
        | );
        |
        |
        |INSERT INTO redistest VALUES('1','200');  --redis对应命令：set 1 100
        |
        |""".stripMargin
  }


  @Test
  def redisLpush(): Unit = {
    sql =
      """
        |
        |CREATE TABLE redistest (
        |    id STRING,
        |    age STRING
        |) WITH (
        | 'connector' = 'redis',
        | 'host'='localhost',
        | 'port' = '8080',
        | 'command'='lpush',
        | 'password'='K6CJDjjnE9d0OxRjYNKZ',
        | 'mode'='cluster'
        | );
        |
        |
        |INSERT INTO redistest VALUES('alist','200'); --redis对应命令：lpush alist 200
        |
        |""".stripMargin
  }

  @Test
  def redisSadd(): Unit = {
    sql =
      """
        |
        |CREATE TABLE redistest (
        |    id STRING,
        |    age STRING
        |) WITH (
        | 'connector' = 'redis',
        | 'host'='localhost',
        | 'port' = '8080',
        | 'command'='sadd',
        | 'password'='sjmobin',
        | 'mode'='cluster'
        | );
        |
        |
        |INSERT INTO redistest VALUES('saddtest','100'); --redsi命令：sadd saddtest 100
        |
        |""".stripMargin
  }

  @Test
  def redisZadd(): Unit = {
    sql =
      """
        |
        |CREATE TABLE redistest (
        |    key STRING,
        |    `element` STRING,
        |    score STRING
        |) WITH (
        | 'connector' = 'redis',
        | 'host'='localhost',
        | 'port' = '8080',
        | 'command'='zadd',
        | 'password'='sjmobin',
        | 'mode'='cluster'
        | );
        |
        |
        |INSERT INTO redistest VALUES('zaddtest','fff','88'); --redis命令：zadd zaddtest 88 ff
        |
        |""".stripMargin
  }


  @Test
  def redisGet(): Unit = {
    sql =
      """
        |
        |CREATE TABLE source_table (
        |       user_id INT,
        |       cost DOUBLE,
        |       ts AS localtimestamp,
        |       procTime AS PROCTIME()
        |) WITH (
        |  'connector' = 'datagen',
        |  'rows-per-second'='5',
        |  'fields.user_id.kind'='random',
        |  'fields.user_id.min'='1',
        |  'fields.user_id.max'='1',
        |  'fields.cost.kind'='random',
        |  'fields.cost.min'='1',
        |  'fields.cost.max'='100'
        |);
        |
        |
        |CREATE TABLE redistest (
        |    id STRING,
        |    school STRING
        |) WITH (
        | 'connector' = 'redis',
        | 'host'='localhost',
        | 'port' = '8080',
        | 'command'='GET',
        | 'password'='sjmobin',
        | 'mode'='cluster'
        | );
        |
        |CREATE TABLE printTable (
        |       id STRING,
        |       school STRING
        |) WITH (
        |    'connector' = 'print'
        |);
        |
        |INSERT INTO printTable
        |SELECT id,school
        |  FROM source_table a
        |  LEFT JOIN
        |       redistest FOR SYSTEM_TIME AS OF a.procTime AS b
        |  ON CAST(a.user_id AS STRING) = b.id;
        |
        |""".stripMargin
  }

  @Test
  def redisGet_FileSystem(): Unit = {
    sql =
      """
        | CREATE  TABLE test1(
        |    user_id STRING,
        |    procTime AS PROCTIME()
        |  ) with (
        |    'connector' = 'filesystem',           -- required: specify the connector
        |   'path' = 'file:///tmp/Mlink/userid.txt',  -- required: path to a directory
        |    'format' = 'csv'
        |  );
        |
        |
        |CREATE TABLE redistest (
        |    id STRING,
        |    school STRING
        |) WITH (
        | 'connector' = 'redis',
        | 'host'='localhost',
        | 'port' = '8080',
        | 'command'='GET',
        | 'password'='mobin',
        | 'mode'='cluster'
        | );
        |
        |CREATE TABLE printTable (
        |       id STRING,
        |       school STRING
        |) WITH (
        |    'connector' = 'print'
        |);
        |
        |INSERT INTO printTable
        |SELECT id,school
        |  FROM test1 a
        |  LEFT JOIN
        |       redistest FOR SYSTEM_TIME AS OF a.procTime AS b
        |  ON CAST(a.user_id AS STRING) = b.id;
        |
        |""".stripMargin
  }

  @Test
  def redisHSet(): Unit = {
    //localhost:8080> hset user1 name mobin
    //(integer) 1
    //localhost:8080> hget user1 name
    //"mobin"
    //localhost:8080> hset user1 age 12
    //(integer) 1
    //localhost:8080> hget user1 age
    //"12"
    sql =
      """
        |
        |CREATE TABLE redistest (
        |    hashkey  STRING,
        |    edcode STRING,
        |    `value` STRING
        |) WITH (
        | 'connector' = 'redis',
        | 'host'='localhost',
        | 'port' = '8080',
        | 'command'='HSET',
        | 'ttl-sec'='60',
        | 'password'='sjmobin',
        | 'mode'='cluster'
        | );
        |
        |--hget db:info:16 edcode
        |
        |
        |INSERT INTO redistest VALUES('op:abc','FC99999','54544515');
        |--redis命令：hset op:abc FC99999 54544515
        |
        |""".stripMargin
  }



  @Test
  def insertDataToHBase(): Unit = {
    sql =
      """
        |
        |set execution.runtime-mode=streaming;
        |CREATE TABLE sink_hbase_ad_effect_cost (
        |                                           rowkey STRING
        |                                           ,cf ROW<activePlace STRING, city STRING,district STRING, osType STRING,province STRING, sexType STRING>
        |                                           ,PRIMARY KEY (rowkey) NOT ENFORCED
        |) WITH (
        |  --     'connector' = 'print'
        |      'connector' = 'hbase-1.4',
        |      'table-name' = 'ad_effect_cost',
        |      'zookeeper.quorum' = 'localhost:2181'
        |      );
        |INSERT INTO sink_hbase_ad_effect_cost VALUES('8888',ROW('1','2','3','4','5','6'));
        |
        |""".stripMargin
  }

  @Test
  def genToPrint(): Unit = {
    sql =
      """
        |
        |CREATE TABLE Orders (
        |    order_number BIGINT,
        |    price        DECIMAL(32,2),
        |    buyer        ROW<first_name STRING, last_name STRING>,
        |    order_time   TIMESTAMP(3)
        |) WITH (
        |  'connector' = 'datagen'
        |);
        |
        |CREATE TABLE Orders_Print (
        |    order_number BIGINT,
        |    price        DECIMAL(32,2),
        |    buyer        ROW<first_name STRING, last_name STRING>
        |) WITH (
        |  'connector' = 'print'
        |);
        |
        |INSERT INTO Orders_Print SELECT order_number,price,buyer FROM Orders; --abc
        |""".stripMargin
  }

  /**
   * A: 1,2,3,4
   * B: SELECT SUM(id1) AS id1,id2,id3,id4 FROM A GROUP BY id2,id3,id4   1,2,3,4
   * C: SELECT SUM(id1),SUM(id2),id3,id4 FROM A GROUP BY id3,id4
   *
   */
  @Test
  def icebergToA(): Unit = {
    sql =
      """
        |--SET execution.runtime-mode = BATCH;
        |--SET 'execution.checkpointing.interval' = '1min';
        |--SET 'execution.checkpointing.min-pause' = '10s';
        |SET 'parallelism.default' = '1';
        |
        | CREATE TABLE A (
        |                                 id1 int,
        |                                 id2 int,
        |                                 id3 int,
        |                                 id4 int
        |)
        |--PARTITIONED BY(stat_date,stat_hour)
        |WITH (
        |      'connector'='iceberg',
        |      'format-version' = '2',
        |      'catalog-name'='hadoop_prod',  --rrgfg
        |      'catalog-type'='hadoop',
        |--           'write.upsert.enabled'='true',
        |--       'write.distribution-mode'='hash',
        |      'warehouse'='file:///tmp/flink/iceberg/'
        |      );
        |
        |INSERT INTO A VALUES(2,2,3,4);
        |
        |
        |""".stripMargin
  }

  @Test
  def icebergToB(): Unit = {
    sql =
      """
        |--SET 'execution.checkpointing.interval' = '1min';
        |--SET 'execution.checkpointing.min-pause' = '10s';
        |SET 'parallelism.default' = '1';
        |
        | CREATE TABLE A (
        |                                 id1 int,
        |                                 id2 int,
        |                                 id3 int,
        |                                 id4 int
        |)
        |--PARTITIONED BY(stat_date,stat_hour)
        |WITH (
        |      'connector'='iceberg',
        |      'format-version' = '2',
        |      'catalog-name'='hadoop_prod',  --rrgfg
        |      'catalog-type'='hadoop',
        |--           'write.upsert.enabled'='true',
        |--       'write.distribution-mode'='hash',
        |      'warehouse'='file:///tmp/flink/iceberg/'
        |      );
        |
        | CREATE TABLE B (
        |                                 id1 int,
        |                                 id2 int,
        |                                 id3 int,
        |                                 id4 int,
        |                                 PRIMARY KEY (id2,id3,id4) NOT ENFORCED
        |)
        |--PARTITIONED BY(stat_date,stat_hour)
        |WITH (
        |      'connector'='iceberg',
        |      'format-version' = '2',
        |      'catalog-name'='hadoop_prod',  --rrgfg
        |      'catalog-type'='hadoop',
        |           'write.upsert.enabled'='true',
        |--       'write.distribution-mode'='hash',
        |      'warehouse'='file:///tmp/flink/iceberg/'
        |      );
        |
        |INSERT INTO B SELECT SUM(id1) AS id1,id2,id3,id4 FROM A /*+ OPTIONS('streaming'='true', 'monitor-interval'='1s')*/ GROUP BY id2,id3,id4;
        |
        |
        |""".stripMargin
  }

  @Test
  def icebergToB_PRINT(): Unit = {
    sql =
      """
        |--SET execution.runtime-mode = BATCH;
        |--SET 'execution.checkpointing.interval' = '1min';
        |--SET 'execution.checkpointing.min-pause' = '10s';
        |SET 'parallelism.default' = '1';
        |
        |
        | CREATE TABLE B (
        |                                 id1 int,
        |                                 id2 int,
        |                                 id3 int,
        |                                 id4 int,
        |                                 PRIMARY KEY (id2,id3,id4) NOT ENFORCED
        |)
        |--PARTITIONED BY(stat_date,stat_hour)
        |WITH (
        |      'connector'='iceberg',
        |      'format-version' = '2',
        |      'catalog-name'='hadoop_prod',  --rrgfg
        |      'catalog-type'='hadoop',
        |           'write.upsert.enabled'='true',
        |--       'write.distribution-mode'='hash',
        |      'warehouse'='file:///tmp/flink/iceberg/'
        |      );
        |
        | CREATE TABLE B_PRINT (
        |                                 id1 int,
        |                                 id2 int,
        |                                 id3 int,
        |                                 id4 int
        |)
        |--PARTITIONED BY(stat_date,stat_hour)
        |WITH (
        |      'connector'='print'
        |      );
        |
        |INSERT INTO B_PRINT SELECT * FROM B/*+ OPTIONS('streaming'='true', 'monitor-interval'='1s')*/;
        |
        |
        |""".stripMargin
  }

}
