package stock.dwd


import com.alibaba.fastjson.JSON
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector
import stock.func.{Stock50FilterFunction, Stock50flatMapFunction, StockHistoryKFilterFunction, StockHistoryKFlatMapFunction, StockIndexTimeLineFilterFunction, StockIndexTimeLineFlatMapFunction, StockRealTimeBatchFilterFunction, StockRealTimeBatchFlatMapFunction}
import stock.utils.MykafkaUtils



/*
* 从ods获取数据，进行分流处理，对数据进行ETL工作
* */
object StockOds2Dwd {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    env.setParallelism(3)

    //从ods获取数据
    val odsStream = env.addSource(MykafkaUtils.getKafkaSource("ods_log_stocks","groupid"))

    //定义各种流的标签
    val StockIndexTimeLine = new OutputTag[String]("StockIndexTimeLine")
    val Stock50 = new OutputTag[String]("Stock50")
    val StockRealTimeBatch = new OutputTag[String]("StockRealTimeBatch")
    val StockHistoryK = new OutputTag[String]("StockHistoryK")

    //分流处理
    val processStream = odsStream.process(new ProcessFunction[String, String] {
      override def processElement(value: String, context: ProcessFunction[String, String]#Context, collector: Collector[String]): Unit = {
        if (!value.equals("") || value != null) {
          if (value.contains("trade_money")) { //包含了某个流里特殊的字段
            context.output(StockHistoryK, value)
          } else if (value.contains("name") && value.contains("indexList")) {
            context.output(StockRealTimeBatch, value)
          } else if (value.contains("remark") && value.contains("dataList")) {
            context.output(StockIndexTimeLine, value)
          } else {
            collector.collect(value)
          }
        }
      }
    })


    /*
    * FlatMapFunction[String,Stock50]，flatmap继承这个方法，包括泛型里面是输入和输出
    *
    * RichFilterFunction[StockHistoryK]  继承RichFilterFunction方法，【】里是输入的数据类型
    * */
    processStream.flatMap(new Stock50flatMapFunction)
      .keyBy(_.dealType)
      .filter(new Stock50FilterFunction)
      .map(JSON.toJSON(_).toString)
      .addSink(MykafkaUtils.getkafkaSink("dwd_stock_50"))

    processStream.getSideOutput(StockHistoryK)
      .flatMap(new StockHistoryKFlatMapFunction)
      .keyBy(_.code)
      .filter(new StockHistoryKFilterFunction)
      .map(JSON.toJSON(_).toString)
      .addSink(MykafkaUtils.getkafkaSink("dwd_stock_history"))

    processStream.getSideOutput(StockRealTimeBatch)
      .flatMap(new StockRealTimeBatchFlatMapFunction)
      .keyBy(_.code)
      .filter(new StockRealTimeBatchFilterFunction)
      .map(JSON.toJSON(_).toString)
      .addSink(MykafkaUtils.getkafkaSink("dwd_stock_realTimeBatch"))

    processStream.getSideOutput(StockIndexTimeLine)
      .flatMap(new StockIndexTimeLineFlatMapFunction)
      .keyBy(_.name)
      .filter(new StockIndexTimeLineFilterFunction)
      .map(JSON.toJSON(_).toString)
      .addSink(MykafkaUtils.getkafkaSink("dwd_stock_indexTimeLine"))



    env.execute()

  }
}
