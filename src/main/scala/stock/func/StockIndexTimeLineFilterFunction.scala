package stock.func

import org.apache.flink.api.common.functions.RichFilterFunction
import org.apache.flink.api.common.state.ValueStateDescriptor
import stock.bean.StockIndexTimeLine

class StockIndexTimeLineFilterFunction extends RichFilterFunction[StockIndexTimeLine]{
  lazy val tsState = getRuntimeContext.getState(new ValueStateDescriptor[Long]("tsState",classOf[Long])) //state要获取上下文的信息，用getRuntimeContext

  override def filter(t: StockIndexTimeLine): Boolean = {
    if (tsState.value()==0 || t.ts >tsState.value()){
      tsState.update(t.ts)
      true
    }else{
      false
    }

  }
}
