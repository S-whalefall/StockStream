package stock.utils

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer, FlinkKafkaProducer}
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer

import java.util.Properties



/*
* kafka工具类
* */
object MykafkaUtils {


  val  brokerList = "master:9092"

  //实现把数据写入kafka
  def writeData2Kafka(topic: String,value:String) ={    //这里是要用生产者，来作为kafka的入口，将数据写入kafka

    val props = new Properties()
    props.setProperty(ProducerConfig.ACKS_CONFIG,"-1")  //kafka返回值为-1，有且只有一条数据
    props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,brokerList)  //连接主机的ip和端口配置
    props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,classOf[StringSerializer].getName)  //将传入的key和value值进行序列化操作
    props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,classOf[StringSerializer].getName)  //kafka返回值为-1，有且只有一条数据

    val kafkaProducer = new KafkaProducer[String, String](props)

    val record = new ProducerRecord[String, String](topic, value)

    kafkaProducer.send(record)  //将数据传出，里面需要ProducerRecord对象作为参数
    kafkaProducer.flush()
    kafkaProducer.close()

  }


  // 获取kafkaSource topic groupId   (Flink 读取这个topic)
  def getKafkaSource(topic: String,groupId:String): Unit ={ //这里是作为消费者组，去拿取kafka中的数据

    val props = new Properties()
    props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,brokerList)
    props.setProperty(ConsumerConfig.GROUP_ID_CONFIG,groupId)
    new FlinkKafkaConsumer[String](topic, new SimpleStringSchema(), props)
  }

  // 获取kafkaSink topic   (Flink 写入这个topic)
  def getkafkaSink(topic:String): Unit ={
    new FlinkKafkaProducer[String](brokerList,topic,new SimpleStringSchema())
  }


}
