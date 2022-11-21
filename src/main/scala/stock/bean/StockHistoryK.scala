package stock.bean

import scala.beans.BeanProperty

case class StockHistoryK(@BeanProperty code:String,
                        @BeanProperty ts:String,
                        @BeanProperty openPrice:Double,
                        @BeanProperty closePrice:Double,
                        @BeanProperty diffPrice:Double
                        )
