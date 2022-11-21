package stock.ods;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import stock.utils.MykafkaUtils;


public class StockData2Ods {

    private static final Logger LOGGER = LoggerFactory.getLogger(StockData2Ods.class);    //Logback方法要使用的对象

    public static void main(String[] args) {

        String topics = "ods_log_stocks";

        //日志落盘
        LOGGER.info(GetStockData.getStockTimeLine().trim());  //.trim()去除数据之前的无效空格
        LOGGER.info(GetStockData.getStock50());
        LOGGER.info(GetStockData.getStockRealTimeBatch());
        LOGGER.info(GetStockData.getStockHistoryK());

        //日志写入kafka
        MykafkaUtils.writeData2Kafka(topics,GetStockData.getStockTimeLine().trim());

        MykafkaUtils.writeData2Kafka(topics,GetStockData.getStock50().trim());

        MykafkaUtils.writeData2Kafka(topics,GetStockData.getStockRealTimeBatch().trim());

        MykafkaUtils.writeData2Kafka(topics,GetStockData.getStockHistoryK().trim());


    }
}
