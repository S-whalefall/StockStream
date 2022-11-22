package stock.dwd

import com.alibaba.fastjson.JSON
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import stock.bean.Stock50
import stock.utils.MykafkaUtils

/*
* 基于股票交易最近50条交易数据，每隔10s，计算过去1分钟内某股票，每种类型成交总数量
*(思路：获取数据，变换类型，设定判断时间，设定分类，设定窗口，计算)
* 并行度也会一定程度上影响
* */
object Stock50Price {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    env.setParallelism(1)

    val dataStream = env.addSource(MykafkaUtils.getKafkaSource("dwd_stock_50", "sunda"))
      .map(line=>{
        JSON.parseObject(line,classOf[Stock50])
      })
      .assignAscendingTimestamps(_.ts*1000L)
      .keyBy(_.dealType)
      .window(SlidingEventTimeWindows.of(Time.minutes(1),Time.seconds(10)))
      .sum("dealCount")

    env.execute()


  }
}
