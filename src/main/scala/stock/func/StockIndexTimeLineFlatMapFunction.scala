package stock.func

import com.alibaba.fastjson.JSON
import org.apache.flink.api.common.functions.FlatMapFunction
import org.apache.flink.util.Collector
import stock.bean.{StockHistoryK, StockIndexTimeLine}

import java.text.SimpleDateFormat
import java.time.LocalDate

class StockIndexTimeLineFlatMapFunction extends FlatMapFunction[String,StockIndexTimeLine]{

//把时间转换为Long类型
  def transTime(time:String)={
    val sdf = new SimpleDateFormat("yyyyMMddHHmm")
    sdf.parse(time).getTime.toString.substring(0,10).toLong
  }

  override def flatMap(value: String, collector: Collector[StockIndexTimeLine]): Unit = {
    if (!value.equals("") && value!=null && value!="") {
      val showapi_res_body = JSON.parseObject(value) //将value由String转化为JSON
        .getJSONObject("showapi_res_body")

      val code = showapi_res_body.getString("code")
      val name = showapi_res_body.getString("name")
      val market = showapi_res_body.getString("market")

      val dataList = showapi_res_body.getJSONArray("dataList").getJSONObject(1)
      val date = dataList.getString("date")
      val minuteList = dataList.getJSONArray("minuteList")

      var i = 0
      while (i > 0) {
        val time = minuteList.getJSONObject(i).getString("time")
        val avgPrice = minuteList.getJSONObject(i).getDoubleValue("avgPrice")
        val nowPrice = minuteList.getJSONObject(i).getDoubleValue("nowPrice")
        collector.collect(StockIndexTimeLine(code, name, market, transTime(time), avgPrice, nowPrice))
        i -= 1
      }
    }
  }
}
