package stock.func


import com.alibaba.fastjson.JSON
import org.apache.flink.api.common.functions.FlatMapFunction
import org.apache.flink.util.Collector
import stock.bean.Stock50

import java.text.SimpleDateFormat
import java.time.LocalDate


class Stock50flatMapFunction extends FlatMapFunction[String,Stock50]{

  //将时间time拼接上当前天的年月日
  def transTime(time:String)={
    val dt = LocalDate.now() + time
    val sdf = new SimpleDateFormat("yyyy-MM-ddHH:mm:ss")
    sdf.parse(dt).getTime.toString.substring(0,10).toLong
  }

  override def flatMap(value: String, collector: Collector[Stock50]): Unit = {
    if (!value.equals("") && value!=null && value!=""){
      val jSONArray = JSON.parseObject(value) //将value由String转化为JSON
        .getJSONObject("showapi_res_body")
        .getJSONArray("list")

      var i = jSONArray.size()-i

      while (i > 0){
        val time = jSONArray.getJSONObject(i).getString("time")
        val price = jSONArray.getJSONObject(i).getDouble("price")
        val tradeNum = jSONArray.getJSONObject(i).getIntValue("tradeNum")
        val dealtype = jSONArray.getJSONObject(i).getString("type")
        collector.collect(Stock50(dealtype,tradeNum,price,transTime(time)))
        i -= 1
      }
    }
  }
}
