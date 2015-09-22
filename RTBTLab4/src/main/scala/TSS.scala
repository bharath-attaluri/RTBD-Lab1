/**
 * Created by Bharath Attaluri on 21-09-2015.
 */
import org.apache.spark.SparkConf
import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
object TSS {
  def main(args: Array[String]) {


    val filters = args

    // Set the system properties so that Twitter4j library used by twitter stream
    // can use them to generate OAuth credentials

    System.setProperty("twitter4j.oauth.consumerKey", "2527D72VTnJcSQZ4NK5MWRlPw")
    System.setProperty("twitter4j.oauth.consumerSecret", "Xdn26CiioH1KdPusB5F0But3nsLSXbco5HTNBGthVjhIL8kNjF")
    System.setProperty("twitter4j.oauth.accessToken", "2530463148-1LoM1VuG3j2XO0xnym8uIIziYdHj42unLYJI3Rt")
    System.setProperty("twitter4j.oauth.accessTokenSecret", "BkvK752QdabzaUMTcHJjjTQspGCEI01iOR5motR7TWt1i")

    //Create a spark configuration with a custom name and master
    // For more master configuration see  https://spark.apache.og/docs/1.2.0/submitting-applications.html#master-urls
    val sparkConf = new SparkConf().setAppName("STweetsApp").setMaster("local[*]").set("spark.driver.memory","4g")
    //Create a Streaming COntext with 2 second window
    val ssc = new StreamingContext(sparkConf, Seconds(2))
    //Using the streaming context, open a twitter stream (By the way you can also use filters)
    //Stream generates a series of random tweets
    val stream = TwitterUtils.createStream(ssc, None, filters)
    stream.print()
    //Map : Retrieving Hash Tags
    val hashTags = stream.flatMap(status => status.getText.split(" ").filter(_.startsWith("#")))

    //Finding the top hash Tags on 30 second window
    val topCounts30 = hashTags.map((_, 1)).reduceByKeyAndWindow(_ + _, Seconds(30))
      .map{case (topic, count) => (count, topic)}
      .transform(_.sortByKey(false))
    //Finding the top hash Tgas on 10 second window
    val topCounts10 = hashTags.map((_, 1)).reduceByKeyAndWindow(_ + _, Seconds(10))
      .map{case (topic, count) => (count, topic)}
      .transform(_.sortByKey(false))

    // Print popular hashtags
    topCounts30.foreachRDD(rdd => {
      val topList = rdd.take(10)
      SocketClient.sendCommandToRobot("Total tweets analyzed is "+ rdd.count())
      println("\nPopular topics in last 30 seconds (%s total):".format(rdd.count()))
      topList.foreach{case (count, tag) => println("%s (%s tweets)".format(tag, count))}
    })

    topCounts10.foreachRDD(rdd => {
      val topList = rdd.take(10)
      SocketClient.sendCommandToRobot("Total tweets analyzed is "+ rdd.count())
      println("\nPopular topics in last 10 seconds (%s total):".format(rdd.count()))
      topList.foreach{case (count, tag) => println("%s (%s tweets)".format(tag, count))}
    })
    ssc.start()

    ssc.awaitTermination()
  }
}