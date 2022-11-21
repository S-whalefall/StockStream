package stock.func

import com.alibaba.fastjson.JSON
import org.apache.flink.api.common.functions.FlatMapFunction
import org.apache.flink.util.Collector
import stock.bean.{Stock50, StockHistoryK}

class StockHistoryKFlatMapFunction extends FlatMapFunction[String,StockHistoryK]{
  override def flatMap(value: String, collector: Collector[StockHistoryK]): Unit = {
    if (!value.equals("") && value!=null && value!=""){
      val jSONArray = JSON.parseObject(value) //将value由String转化为JSON
        .getJSONObject("showapi_res_body")
        .getJSONArray("list")

      var i = jSONArray.size()-i

      while (i > 0){
        val code = jSONArray.getJSONObject(i).getString("code")
        val ts = jSONArray.getJSONObject(i).getString("date")
        val open_price = jSONArray.getJSONObject(i).getDouble("open_price")
        val close_price = jSONArray.getJSONObject(i).getDouble("close_price")
        val diff_price = close_price - open_price
        collector.collect(StockHistoryK(code, ts, open_price, close_price , diff_price))
        i -= 1
      }
    }

  }
}
