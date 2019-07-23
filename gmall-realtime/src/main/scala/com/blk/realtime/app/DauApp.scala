package com.blk.realtime.app

import java.text.SimpleDateFormat
import java.util
import java.util.Date

import com.alibaba.fastjson.{JSON, JSONObject}
import com.blk.common.GmallConstants
import com.blk.realtime.bean.StartUpLog
import com.blk.realtime.utils.{MyKafkaUtil, RedisUtil}
import org.apache.hadoop.conf.Configuration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis
import org.apache.phoenix.spark._

object DauApp {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[*]").setAppName("DauApp")

    val ssc = new StreamingContext(conf, Seconds(5))

    //消费kafka
    val inputDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstants.KAFKA_TOPIC_STARTUP, ssc)

    /*inputDStream.foreachRDD{
      rdd => {
        println(rdd.map(_.value()).collect().mkString("\n"))
      }
    }*/

    // 2 结构转换成case class 补充两个时间字段
    val startUpLogDStream: DStream[StartUpLog] = inputDStream.map {
      record => {
        val jsonString: String = record.value()
        val startUpLog: StartUpLog = JSON.parseObject(jsonString, classOf[StartUpLog])

        val ts: Long = startUpLog.ts

        val sdf = new SimpleDateFormat("yyyy-MM-dd HH")
        val dateTimeString = sdf.format(new Date(ts))

        val dateAndHour: Array[String] = dateTimeString.split(" ")

        startUpLog.logDate = dateAndHour(0)
        startUpLog.logHour = dateAndHour(1)
        startUpLog
      }
    }


    //2  去重  根据今天访问过的用户清单进行过滤
    val filterDStream: DStream[StartUpLog] = startUpLogDStream.transform {
      rdd => {

        //创建jedis客户端
        val client: Jedis = RedisUtil.getJedisClient

        val sdf = new SimpleDateFormat("yyyy-MM-dd")
        val date = sdf.format(new Date())

        //获取用户清单
        val midSet: util.Set[String] = client.smembers("dau:" + date)

        val midSetBroadcast: Broadcast[util.Set[String]] = ssc.sparkContext.broadcast(midSet)

        //关闭客户端
        client.close()

        val filterRDD = rdd.filter {
          startUpLog => {
            !midSetBroadcast.value.contains(startUpLog.mid)
          }
        }
        filterRDD
      }
    }

    //本批次内进行去重
    val groupDStream: DStream[(String, Iterable[StartUpLog])] = filterDStream.map(start => (start.mid, start)).groupByKey()

    val flatDStream: DStream[StartUpLog] = groupDStream.flatMap {
      case (mid, iter) => {
        iter.toList.sortBy(_.ts).take(1)
      }
    }

    /*val flatDStream: DStream[StartupLog] = filterDStream.map(startuplog => (startuplog.mid, startuplog)).groupByKey().flatMap { case (mid, startupLogItr) =>
      startupLogItr.take(1)
    }*/


    // 3 把所有今天访问过的用户保存起来  保存到Redis
    flatDStream.foreachRDD(rdd => {
      rdd.foreachPartition {
        iter => {
          //创建jedis客户端
          val client: Jedis = RedisUtil.getJedisClient

          for (startLog <- iter) {
            client.sadd("dau:" + startLog.logDate, startLog.mid)
            println(startLog)
          }

          //关闭客户端
          client.close()
        }
      }
    })


    //保存到HBase
    flatDStream.foreachRDD {
      rdd => {
        rdd.saveToPhoenix("GMALL2019_DAU",
          Seq("MID", "UID", "APPID", "AREA", "OS", "CH", "TYPE", "VS", "LOGDATE", "LOGHOUR", "TS"),
          new Configuration(),
          Some("hadoop102,hadoop103,hadoop104:2181"))
      }
    }


    ssc.start()
    ssc.awaitTermination()
  }
}
