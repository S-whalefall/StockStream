package stock.bean

import scala.beans.BeanProperty

case class StockIndexTimeLine(@BeanProperty code:String,
                              @BeanProperty name:String,
                              @BeanProperty market:String,
                              @BeanProperty ts:Long,
                              @BeanProperty avgPrice:Double,
                              @BeanProperty nowPrice:Double
                             )
