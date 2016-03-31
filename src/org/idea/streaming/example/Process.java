package org.idea.streaming.example;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

import com.google.common.collect.Lists;

import scala.Tuple2;

import java.util.*;
import java.util.regex.Pattern;

public class Process {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		final Pattern SPACE = Pattern.compile(" ");

		System.out.println("----------------------------------------------");
		System.out.println("-----------Stream Analysis Starting-----------");
		System.out.println("----------------------------------------------");

		// if (args.length != 3) {
		// System.err.println("Need to pass KafkaIP, sparkIP, topic");
		// System.exit(1);
		// }
		String kafkaIP = "localhost:9092";
		//String sparkIP = "local[2]";
		String sparkIP = "spark://Surbhis-MacBook-Pro.local:7077";
		String topic = "test1";

		String consumerGroup = "my-consumer-group";

		int numThreads = 5;
		// create context

		 JavaStreamingContext jsc = null;
		//
		//
		 jsc = new JavaStreamingContext(sparkIP, "Process Class", new
		 Duration(30000), System.getenv("SPARK_HOME"),
		 JavaStreamingContext.jarOfClass(Process.class));
		 jsc.checkpoint("../src/checkpoint/");

//		SparkConf sparkConf = new SparkConf().setAppName("JavaWordCount").setMaster(sparkIP)
//				.set("spark.executor.memory", "1g");
//		JavaStreamingContext jsc = new JavaStreamingContext(sparkConf, new Duration(20000));

		System.out.println("Past JSC");

		HashMap<String, Integer> topicMap = new HashMap<>();
		String[] topics = topic.split(",");
		for (String topicName : topics) {
			topicMap.put(topicName, numThreads);
		}

		System.out.println("Topics: " + topicMap);

		JavaPairDStream<String, String> messages = KafkaUtils.createStream(jsc, kafkaIP, consumerGroup, topicMap);

		System.out.println("KafkaIP: " + kafkaIP);
		System.out.println("----------------------------------------------");

		JavaDStream<String> lines = messages.map(new Function<Tuple2<String, String>, String>() {
			@Override
			public String call(Tuple2<String, String> tuple2) {
				return tuple2._2();
			}
		});

		JavaDStream<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
			@Override
			public Iterable<String> call(String x) {
				return Lists.newArrayList(SPACE.split(x));
			}
		});

		JavaPairDStream<String, Integer> wordCounts = words.mapToPair(new PairFunction<String, String, Integer>() {
			@Override
			public Tuple2<String, Integer> call(String s) {
				return new Tuple2<String, Integer>(s, 1);
			}
		}).reduceByKey(new Function2<Integer, Integer, Integer>() {
			@Override
			public Integer call(Integer i1, Integer i2) {
				return i1 + i2;
			}
		});

		int w = wordCounts.hashCode();
		System.out.println(wordCounts.toJavaDStream());
		
		wordCounts.print();

		// Start the computation
		jsc.start();
		jsc.awaitTermination();

	}

}
