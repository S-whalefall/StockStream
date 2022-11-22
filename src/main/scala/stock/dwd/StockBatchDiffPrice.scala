package stock.dwd

import com.alibaba.fastjson.JSON
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector
import stock.bean.StockRealTimeBatch
import stock.utils.MykafkaUtils


/*
* 基于实时批量查询，实时展示，如果各指数当时的价格比当天最高价格相差了5，就输出提示信息
* 思路:
*   把最大的价格维护到状态中
*   判断是否是第一条数据，直接更新状态
* 如果不是第一条数据，如果比状态中维护的价格（最高价格）高，更新状态值
*                 如果比状态中维护的价格（最高价格）低，做差值，是否相差了5，超过5就提示
* */
object StockBatchDiffPrice {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    env.setParallelism(1)

    env.addSource(MykafkaUtils.getKafkaSource("dwd_stock_realTimeBatch", "sunda"))
      .map(JSON.parseObject(_,classOf[StockRealTimeBatch]))
      .keyBy(_.name)
      .process(new StockBatchDiffPriceKeysProcessFunc)
      .print()

    env.execute()
  }
}

class StockBatchDiffPriceKeysProcessFunc extends KeyedProcessFunction[String,StockRealTimeBatch,String]{

  // 定义状态维护最高价格
  var maxPriceState:ValueState[Double] = _

  override def open(parameters: Configuration): Unit = {
    getRuntimeContext.getState(new ValueStateDescriptor[Double]("maxPriceState",classOf[Double],0))
  }

  override def processElement(value: StockRealTimeBatch, ctx: KeyedProcessFunction[String, StockRealTimeBatch, String]#Context, out: Collector[String]): Unit = {
    //判断是否是第一条数据，直接更新状态
    if (maxPriceState.value() == 0){
      maxPriceState.update(value.nowPrice)
    }else{
      //如果比状态中维护的价格（最高价格）高，更新状态值
      if (value.nowPrice > maxPriceState.value()){
        maxPriceState.update(value.nowPrice)
      }else{
        //如果比状态中维护的价格（最高价格）低，做差值，是否相差了5，超过5就提示
        val diff = maxPriceState.value() - value.nowPrice
        if (diff >= 5){
          out.collect(ctx.getCurrentKey +"跌幅已经超过了，正好加仓好机会")
        }
      }
    }
  }
}
