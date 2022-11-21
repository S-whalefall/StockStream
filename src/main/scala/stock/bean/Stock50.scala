package stock.bean

import scala.beans.BeanProperty

//@BeanProperty 为了解决Json和scla对象之间的转换问题
case class Stock50(
                    @BeanProperty dealType:String,
                    @BeanProperty dealCount:Int,
                    @BeanProperty avgPrice:Double,
                    @BeanProperty ts:Long,
                  )
