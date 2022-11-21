package stock.func

import com.alibaba.fastjson.JSON
import com.ibm.icu.text.SimpleDateFormat
import org.apache.flink.api.common.functions.FlatMapFunction
import org.apache.flink.util.Collector
import stock.bean.{Stock50, StockRealTimeBatch}

class StockRealTimeBatchFlatMapFunction extends FlatMapFunction[String,StockRealTimeBatch]{

  def transTime(time:String)={
    val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    sdf.parse(time).getTime.toString.substring(0,10).toLong
  }

  override def flatMap(value: String, collector: Collector[StockRealTimeBatch]): Unit = {
    if (!value.equals("") && value!=null && value!=""){
      val jSONArray = JSON.parseObject(value) //将value由String转化为JSON
        .getJSONObject("showapi_res_body")
        .getJSONArray("list")

      var i = jSONArray.size()-i

      while (i > 0){
        val market = jSONArray.getJSONObject(i).getString("market")
        val name = jSONArray.getJSONObject(i).getString("name")
        val code = jSONArray.getJSONObject(i).getString("code")
        val nowPrice = jSONArray.getJSONObject(i).getDouble("nowPrice")
        val tradeAmount = jSONArray.getJSONObject(i).getLongValue("tradeAmoint")
        val time = jSONArray.getJSONObject(i).getString("time")
        collector.collect(StockRealTimeBatch(market , name , code, nowPrice, tradeAmount , transTime(time)))
        i -= 1
      }
    }

  }
}
