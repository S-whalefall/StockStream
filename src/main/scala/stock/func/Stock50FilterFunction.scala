package stock.func

import org.apache.flink.api.common.functions.RichFilterFunction
import org.apache.flink.api.common.state.ValueStateDescriptor
import stock.bean.Stock50

class Stock50FilterFunction extends RichFilterFunction[Stock50] {

  lazy val tsState = getRuntimeContext.getState(new ValueStateDescriptor[Long]("tsState",classOf[Long])) //state要获取上下文的信息，用getRuntimeContext

  override def filter(t: Stock50): Boolean = {
    if (tsState.value()==0 || t.ts >tsState.value()){
      tsState.update(t.ts)
      true
    }else{
      false
    }

  }
}
