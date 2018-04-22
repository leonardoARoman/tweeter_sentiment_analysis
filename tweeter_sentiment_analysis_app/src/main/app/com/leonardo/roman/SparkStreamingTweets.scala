package com.leonardo.roman

import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._

import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.twitter.TwitterUtils

import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession

object SparkStreamingTweets {
	def main(args: Array[String]){
		if( args.length < 4){
			println("Error!") 
		}
		StreamingExamples.setStreamingLogLevels()
		println("Hello Spark-Scala")
		var i = 0
		for(i <- 0 to 3 ){
			println("Args["+i+"]: "+args.apply(i))
		}

		val Array(consumerKey, consumerSecret,accessToken,accessTokenSecret) = args.take(4)
		val filters = args.takeRight(args.length - 4)

		System.setProperty("twitter4j.oauth.consumerKey", consumerKey)
		System.setProperty("twitter4j.oauth.consumerSecret", consumerSecret)
		System.setProperty("twitter4j.oauth.accessToken", accessToken)
		System.setProperty("twitter4j.oauth.accessTokenSecret", accessTokenSecret)

		val sparkConf = new SparkConf().setAppName("tweeter_sentiment_analysis").setMaster("local[2]")
		val stream_sc = new StreamingContext(sparkConf, Seconds(60))
		val stream = TwitterUtils.createStream(stream_sc, None)
		val hashTags = stream.map(status => status.getText())//.filter(_.startsWith("#")))
		hashTags.saveAsTextFiles("/Users/leonardoroman/Desktop/data_science_projects/intro_to_DS/tweeter_sentiment_analysis/streamed_data/60sc_data")

		/*
		val stream = TwitterUtils.createStream(stream_sc, None, filters)
		val hashTags = stream.flatMap(status => status.getText.split(" ").filter(_.startsWith("#")))

		val topCount60 = hashTags
		                .map((_,1))
		                .reduceByKeyAndWindow(_+_, Seconds(60))
				            .map{case (topic, count) => (count, topic)}
		                .transform(_.sortByKey(false))



		topCount60.foreachRDD(rdd => {
			    val topList = rdd.take(10)
					println("Popular topics in last 60 seconds (%s total):".format(rdd.count()))
					topList.foreach{case (count, tag) => println("%s (%s tweets)".format(tag, count))}
			    topCount60.saveAsTextFiles("/Users/leonardoroman/Desktop/data_science_projects/intro_to_DS/tweeter_sentiment_analysis/streamed_data/60sc_data")
		})
		*/
		
		stream_sc.start()
		stream_sc.awaitTermination()
	}
}