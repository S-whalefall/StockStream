package stock.bean

import scala.beans.BeanProperty

case class StockRealTimeBatch(@BeanProperty market:String,
                              @BeanProperty name:String,
                              @BeanProperty code:String,
                              @BeanProperty nowPrice:Double,
                              @BeanProperty tradeAmount:Double,
                              @BeanProperty ts:Long
                             )
