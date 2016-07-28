package org.littlewings.spark

import java.io.StringReader
import org.apache.spark.SparkContext._
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.rdd.RDD._
import org.apache.spark.streaming.{Durations, StreamingContext}
//import org.apache.spark.streaming.kafka._
import java.util.Properties
import org.apache.spark._

object KafkaStreaming {

  def main(args: Array[String]): Unit = {


    println("\n====================== Start. ======================")
    //    logger.debug(s"Some message!")
//    val config = new java.util.Properties
//    config.load(this.getClass().getClassLoader().getResourceAsStream("config.properties"))

//    System.setProperty("twitter4j.oauth.consumerKey", config.get("oauth.consumerKey").toString)
//    System.setProperty("twitter4j.oauth.consumerSecret", config.get("oauth.consumerSecret").toString)
//    System.setProperty("twitter4j.oauth.accessToken", config.get("oauth.accessToken").toString)
//    System.setProperty("twitter4j.oauth.accessTokenSecret", config.get("oauth.accessTokenSecret").toString)

    //sparkconf 設定
    val conf = new SparkConf().setAppName("Kafka Streaming")
    val minuteunit: Long = if (args(0).isEmpty) 5 else args(0).toLong
    val ssc = new StreamingContext(conf, Durations.minutes(minuteunit))

    println("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
    // kafka
    val kafkaStream = KafkaUtils.createStream(ssc, "devblog-kafka:2181", "default", Map("syslog-topic" -> 1))
    val kafkaInputCount = kafkaStream.count()
    kafkaInputCount.print
    println("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb")
//    val lines = ssc.socketTextStream("devblog-kafka", 9999)
//    val words = lines.flatMap(_.split(" "))
//    val pairs = words.map(word => (word, 1))
//    val wordCounts = pairs.reduceByKey(_ + _)

//    val tweetRDD = stream
//      .flatMap { status =>
//        val text = status.getText.replaceAll("http(s*)://(.*)/", "").replaceAll("¥¥uff57", "").replaceAll(args(1).toString, "")
//        val analyzer = new JapaneseAnalyzer
//        val tokenStream = analyzer.tokenStream("", text)
//        val baseForm = tokenStream.addAttribute(classOf[BaseFormAttribute])
//        val partOfSpeech = tokenStream.addAttribute(classOf[PartOfSpeechAttribute])
//        val charAttr = tokenStream.addAttribute(classOf[CharTermAttribute])
//
//        //resetメソッドを呼んだ後に、incrementTokenメソッドでTokenを読み進めていく
//        tokenStream.reset()
//
//        try {
//          Iterator
//            .continually(tokenStream.incrementToken())
//            .takeWhile(identity)
//            // 品詞が名詞のものだけ抽出する
//            .map(_ => PartOfSpeechCheckConvert(partOfSpeech.getPartOfSpeech(), charAttr.toString))
//            .toVector
//        } finally {
//          tokenStream.end()
//        }
//      }
//
//    // ２桁以上の文字を対象にアルファベット、数値のみはngwordという単語とする
//    val wordAndOnePairRDD = tweetRDD.map(word => (ngwordConvert(word)))
//    // (Apache, 1) (Spark, 1) というペアにします。ngwordはゴミ単語なので0を設定
//    val ngwordRDD = wordAndOnePairRDD.map(word => (word, word match {
//      case ("ngword") => 0
//      case _ => 1
//    }))

    // countup reduceByKey(_ + _) は　reduceByKey((x, y) => x + y) と等価です。
//    val wordAndCountRDD = ngwordRDD.reduceByKey((a, b) => a + b)
    //    val wordAndCountRDD = wordAndOnePairRDD.reduceByKey(_ + _)

    // key => value value => keyに変更
//    val countAndWordRDD = wordAndCountRDD.map { wordAndWount => (wordAndWount._2, wordAndWount._1) }

    // sort transformをかまさないとsortByKeyが使えない
//    val sortedCWRDD = countAndWordRDD.transform(rdd => rdd.sortByKey(false))

    // value => key key => valueに変更
//    val sortedCountAndWordRDD = sortedCWRDD.map { countAndWord => (countAndWord._2, countAndWord._1) }

    // データ保存先をconfigから取得してdevとliveで保存先切り替える
//    sortedCountAndWordRDD.saveAsTextFiles(config.get("save.file.dir").toString + args(1))
    //    sortedCountAndWordRDD.print()

    // streaming start
//    ssc.start()
//    ssc.awaitTermination()
  }

//  def PartOfSpeechCheckConvert(part: String, word: String) = {
//    if (part.split("-")(0) == "名詞") word else "ngword"
//  }

//  def ngwordConvert(word: String) = {
//    if (word.length >= 2) word.replaceAll("(^[a-z]+$)", "ngword").replaceAll("^[0-9]+$", "ngword") else "ngword"
//  }
}
