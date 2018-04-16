package com.leonardo.roman
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import java.lang.Math
import java.lang.Math

object SparkStreamingTweets {
	def main(args: Array[String]){
		// 1. Create Spark configuration
		val conf = new SparkConf()
				.setAppName("SparkMe Application")
				.setMaster("local[*]")  // local mode

				// 2. Create Spark context
				val sc = new SparkContext(conf)

				val text_file = sc.textFile("/Users/leonardoroman/Desktop/data_science_projects/intro_to_DS/spark_mapreduce_tutorial/big_file.txt")
				val counts = text_file.flatMap(line => line.split(" "))
				.map(word => (word, 1))
				.reduceByKey(_+_)

				counts.count()
				counts.collect()
	}
}