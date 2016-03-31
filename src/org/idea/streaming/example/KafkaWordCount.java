package org.idea.streaming.example;

import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Pattern;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
//import org.apache.spark.examples.streaming.StreamingExamples;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.codehaus.jettison.json.JSONObject;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.google.common.collect.Lists;

import scala.Tuple2;

public final class KafkaWordCount {

	private static final Pattern SPACE = Pattern.compile(" ");

	private KafkaWordCount() {

	}

	public static void main(String[] argsold) throws Exception {
		MqttConsumerToKafkaProducer obj = new MqttConsumerToKafkaProducer();
		obj.start();
		String zkHosts = "localhost";
		String listenTopics = "topic";
		String listenerName = "testListener";
		SparkConf sparkConf = new SparkConf().setAppName("JavaWordCount").setMaster("local[2]")
				.set("spark.executor.memory", "1g");
		JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, new Duration(30000));

		/*
		 * Setting the spark executor memory and local[2] are very important to
		 * avoid the following error: Initial job has not accepted any
		 * resources; check your cluster UI to ensure that workers are
		 * registered and have sufficient resources
		 * 
		 */

		int numThreads = 5;
		final AtomicLong dataCounter = new AtomicLong(0);
		Map<String, Integer> topicMap = new HashMap<String, Integer>();
		Map<String, LightTracker> lightsMap = new HashMap<String, LightTracker>();
		String[] topics = listenTopics.split(",");
		for (String topic : topics) {
			topicMap.put(topic, numThreads);
		}
		JavaPairReceiverInputDStream<String, String> messages = KafkaUtils.createStream(jssc, zkHosts, listenerName,
				topicMap);
		JavaDStream<String> lines = messages.map(new Function<Tuple2<String, String>, String>() {
			public String call(Tuple2<String, String> tuple2) {
				return tuple2._2();
			}
		});

		readRDD(lines);

		JavaDStream<String> lightLines = lines.filter(new Function<String, Boolean>() {
			public Boolean call(String messages) {
				return messages.contains("Lighting");
			}
		});

		JavaDStream<String> lightsOnLines = lightLines.filter(new Function<String, Boolean>() {
			public Boolean call(String messages) {
				return messages.contains("Green");
			}
		});

		JavaDStream<String> lightsOffLines = lightLines.filter(new Function<String, Boolean>() {
			public Boolean call(String messages) {
				return messages.contains("Red");
			}
		});

		// readRDD(lightsOnLines);

		jssc.start();
		jssc.awaitTermination();

	}

	public static void readRDD(JavaDStream<String> dStream) {

		/*
		 * Iterating over all the RDD's in JavaDStream to append timestamp at
		 * the end for processing.
		 */

		dStream.foreachRDD(new Function<JavaRDD<String>, Void>() {
			@Override
			public Void call(JavaRDD<String> rdd) throws Exception {
				JavaRDD<String> rowRDD = rdd.map(new Function<String, String>() {
					/*
					 * Make modifications to the String here.
					 */
					@Override
					public String call(String string) throws Exception {

						return string;
					}
				});
				List<String> ls = rowRDD.collect();
				ObjectMapper mapper = new ObjectMapper();
				for (int i = 0; i < ls.size(); i++) {
					/*
					 * 
					 * Printing the RDD's as JSON Object
					 * 
					 */
					Object json = mapper.readValue(ls.get(i), Object.class);
					mapper.configure(SerializationFeature.INDENT_OUTPUT, true);
					System.out.println(mapper.writerWithDefaultPrettyPrinter().writeValueAsString(json));
				}
				return null;
			}
		});
	}
}