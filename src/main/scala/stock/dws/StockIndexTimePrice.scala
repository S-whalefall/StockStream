package stock.dws

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
/*
* 使用TableAPI或者SQL实现基于股指实时分时线图,将time分钟数匹配到date字段
* 实时显示当前价格和平均价格
* */

object StockIndexTimePrice {
  def main(args: Array[String]): Unit = {


    //创建 TableEnvironment环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = StreamTableEnvironment.create(env)

    //创建一张输入表，并且关联外部组件    Kafka 连接器提供从 Kafka topic 中消费和写入数据的能力。
    tEnv.executeSql(
      """
        |CREATE TABLE KafkaTable_input (
        |  `code` STRING,
        |  `name` STRING,
        |  `market` STRING,
        |  `ts` BIGINT,
        |  avgPrice DOUBLE,
        |  nowPrice DOUBLE,
        |) WITH (
        |  'connector' = 'kafka',
        |  'topic' = 'dwd_stock_indexTimeLine',
        |  'properties.bootstrap.servers' = 'master:9092',
        |  'properties.group.id' = 'sunda',
        |  'format' = 'json'
        |  'json.fail-on-missing-field' = 'false',
        |  'json.ignore-parse-errors' = 'true'
        |)
        |""".stripMargin)

    //创建一张输出表，并且关联外部组件  Upsert Kafka 连接器支持以 upsert 方式从 Kafka topic 中读取数据并将数据写入 Kafka topic。
    tEnv.executeSql(
      """
        |CREATE TABLE KafkaTable_output (
        |  `code` STRING,
        |  `avgPrice` DOUBLE,
        |  `nowPrice` DOUBLE,
        |  PRIMARY KEY (code) NOT ENFORCED
        |
        |) WITH (
        |  'connector' = 'upsert-kafka',
        |  'topic' = 'dwd_stock_index',
        |  'properties.bootstrap.servers' = 'master:9092',
        |  'properties.group.id' = 'sunda',
        |    'key.format' = 'json',
        |  'value.format' = 'json'
        |)
        |""".stripMargin)


    //基于输入表查询操作
    val result = tEnv.sqlQuery(
      """
        |insert into KafkaTable_output
        |select code,avgPrice,nowPrice
        |from t_input
        |""".stripMargin)

    //把查询结果插入到输出表
//    result.executeInsert("KafkaTable_output")
    result.executeInsert("KafkaTable_output")


    env.execute()

  }
}
