package org.idea.streaming.example;

import java.net.UnknownHostException;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Pattern;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
//import org.apache.spark.examples.streaming.StreamingExamples;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.MongoClient;

import scala.Tuple2;

public final class KafkaWordCount {

	private static final Pattern SPACE = Pattern.compile(" ");

	private KafkaWordCount() {

	}

	private static MongoClient mongo = null;
	private static DB db = null;
	private static DBCollection collection = null;

	public static HashMap<String, Lighting> objectHashmap = new HashMap<String, Lighting>();

	public static void main(String[] argsold) throws Exception {
		// MqttConsumerToKafkaProducer obj = new MqttConsumerToKafkaProducer();
		// obj.start();
		Logger.getLogger("org").setLevel(Level.OFF);
		Logger.getLogger("akka").setLevel(Level.OFF);
		TestProducer obj = new TestProducer();
		obj.start();
		String zkHosts = "localhost";
		String listenTopics = "topic";
		String listenerName = "testListener";
		SparkConf sparkConf = new SparkConf().setAppName("JavaWordCount").setMaster("local[2]")
				.set("spark.executor.memory", "1g");
		JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, new Duration(2000));
		// JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
		/*
		 * Setting the spark executor memory and local[2] are very important to
		 * avoid the following error: Initial job has not accepted any
		 * resources; check your cluster UI to ensure that workers are
		 * registered and have sufficient resources
		 * 
		 */
		try {
			mongo = new MongoClient("localhost", 27017);
			db = mongo.getDB("ideadb");
			collection = db.getCollection("lights");
		} catch (UnknownHostException e) {
			e.printStackTrace();
		}

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
		// JavaRDD<String> textFile = sparkContext.textFile("Stream.txt");
		// JavaRDD<String> filtered = textFile.filter(new Function<String,
		// Boolean>() {
		// public Boolean call(String messages) {
		// return messages.contains("Lighting");
		// }
		// });
		// readRDDOne(filtered);
		// readRDD(lines);

		JavaDStream<String> lightLines = lines.filter(new Function<String, Boolean>() {
			public Boolean call(String messages) {
				return messages.contains("Lighting");
			}
		});
		readRDD(lightLines);
		/*
		 * JavaDStream<String> lightsOnLines = lightLines.filter(new
		 * Function<String, Boolean>() { public Boolean call(String messages) {
		 * return messages.contains("Green"); } });
		 * 
		 * JavaDStream<String> lightsOffLines = lightLines.filter(new
		 * Function<String, Boolean>() { public Boolean call(String messages) {
		 * return messages.contains("Red"); } });
		 */
		// readRDD(lightsOnLines);
		Timer timer = new Timer();
		TimerTask hourlyTask = new TimerTask() {

			@Override
			public void run() {
				persistData();
			}

		};
		timer.schedule(hourlyTask, 0l, 1000 * 1);

		jssc.start();
		jssc.awaitTermination();

	}

	public static void persistData() {

		/*
		 * Algorithm to persist data into MongoDB data base 1. Iterate over the
		 * objectHashmap for every key value present in it. 2. For each entry in
		 * hashmap 2.1 Search MongoDB for the key 2.2 if key is present then
		 * 2.2.1 replace the old onTime with new update onTime 2.3 else 2.3.1
		 * make an entry in the mongodb database with key and current onTime.
		 */

		// Hashmap will be stored in MongoDB here
		BasicDBObject query = new BasicDBObject();
		BasicDBObject newDoc = new BasicDBObject();
		BasicDBObject retreivalObj = null;
		BasicDBObject updateObj = null;
		DBCursor cursor = null;
		long onTimeOld = 0;
		long onTimeCurrent = 0;
		String bulbName = null;
		// System.out.println("Priting objectHashmap inside persist method");
		for (Map.Entry<String, Lighting> entry : objectHashmap.entrySet()) {

			// System.out.println(entry.getKey()+" : "+entry.getValue());
		}
		Iterator itr = objectHashmap.entrySet().iterator();

		while (itr.hasNext()) {
			// fidn the bulb entry in mongo
			Map.Entry<String, Lighting> pair = (Map.Entry) itr.next();
			bulbName = pair.getKey();
			onTimeCurrent = pair.getValue().getOnTime();
			// System.out.println("bulbName = "+ bulbName + " onTimeCurrent = "+
			// onTimeCurrent);
			query.put("name", bulbName);
			cursor = collection.find(query);

			// keep a document ready with the name add the time diff later
			// newDoc.put("name", bulbName);

			if (cursor.hasNext()) {
				// When database already has an entry
				// 1. get the entry from db
				// 2. update onTime with current time.
				retreivalObj = (BasicDBObject) cursor.next();
				// newDoc.put("onTime", (onTimeCurrent));

				// update
				// updateObj = new BasicDBObject();
				// updateObj.put("$set", newDoc);
				collection.update(retreivalObj, new BasicDBObject("$set", new BasicDBObject("onTime", onTimeCurrent)));
				// System.out.println("Light " + bulbName+" updated with time "+
				// onTimeCurrent);

			} else {
				// insert
				newDoc.put("name", bulbName);
				newDoc.put("onTime", (onTimeCurrent));
				collection.save(newDoc);

			}
		}

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
						// checkUpdate(string);
						System.out.println(string);
						JSONObject jobj = new JSONObject(string);
						String name = jobj.getString("name");
						String currentState = jobj.getString("state");
						Timestamp timestamp = Timestamp.valueOf(jobj.getString("TimeStamp"));
						Calendar temp = Calendar.getInstance();
						temp.set(Calendar.HOUR, 0);
						temp.set(Calendar.MINUTE, 0);
						temp.set(Calendar.SECOND, 0);
						temp.set(Calendar.MILLISECOND, 0);
						long onTime = (long) 0;

						if (objectHashmap.containsKey(name)) {
							// System.out.println("objectHashmap has " + name);
							Lighting light = objectHashmap.get(name);
							String initialState = light.getIntialState();
							if (!initialState.equals(currentState)) {
								System.out.println("State changed for " + name);
								if (initialState.equals("Red") && currentState.equals("Green")) {

									/*
									 * State changed from Red to Green update
									 * initialState, and store the timeStamp
									 * 
									 */

									System.out.println(light.getName() + " has changed from red to green at: "
											+ light.getTimestamp().getTime());
									light.setTimestamp(timestamp);
									light.setIntialState(currentState);
									persistData();
								} else if (initialState.equals("Green") && currentState.equals("Red")) {

									/*
									 * State changed from Green to Red update
									 * timestamp to new timestamp, update Ontime
									 * for light, update initialState.
									 */
									System.out.println(light.getName() + " has changed from green to red at: "
											+ light.getTimestamp().getTime());
									// The total time for which the light was
									// on.
									long diff = timestamp.getTime() - light.getTimestamp().getTime();
									System.out.println(light.getTimestamp().getTime() + "  - " + timestamp.getTime()
											+ "  diff is = " + diff);
									long oldTimeInMilli = light.getOnTime();
									System.out.println("Old onTime = " + oldTimeInMilli);
									long temp1 = oldTimeInMilli + diff;
									System.out.println("temp1 = " + temp1);

									light.setOnTime(temp1);
									System.out.println(TimeUnit.MILLISECONDS.toMinutes(temp1));
									// System.out.println(
									// String.format("%d min, %d seconds",
									// TimeUnit.MILLISECONDS.toMinutes(temp1),
									// TimeUnit.MILLISECONDS.toSeconds(temp1) -
									// TimeUnit.MILLISECONDS
									// .toSeconds(TimeUnit.MILLISECONDS.toMinutes(temp1))));
									System.out.println(
											String.format("%02d:%02d:%02d", TimeUnit.MILLISECONDS.toHours(temp1),
													TimeUnit.MILLISECONDS.toMinutes(temp1) - TimeUnit.HOURS
															.toMinutes(TimeUnit.MILLISECONDS.toHours(temp1)),
													TimeUnit.MILLISECONDS.toSeconds(temp1) - TimeUnit.MINUTES
															.toSeconds(TimeUnit.MILLISECONDS.toMinutes(temp1))));
									System.out.println("New onTime = " + light.getOnTime());
									System.out.println(
											"New Updated onTime for " + light.getName() + " is " + light.getOnTime());
									light.setTimestamp(timestamp);
									light.setIntialState(currentState);
									persistData();
								}

							} else {
								System.out.println("No Change of state in " + name);
							}
						} else {
							Lighting light = new Lighting();
							light.setName(name);
							light.setOnTime(onTime);
							light.setTimestamp(timestamp);
							light.setIntialState(currentState);
							objectHashmap.put(name, light);
							persistData();
							System.out
									.println("Light added: " + light.getName() + " with onTime: " + light.getOnTime());
						}
						return string;
					}
				});

				/*
				 * //print objecHashmap for (Map.Entry<String, Lighting> entry :
				 * objectHashmap.entrySet()) { System.out.println(
				 * "Priting objectHashmap"); System.out.println(entry.getKey()+
				 * " : "+entry.getValue()); }
				 */
				List<String> ls = rowRDD.collect();
				ObjectMapper mapper = new ObjectMapper();
				/*
				 * for (int i = 0; i < ls.size(); i++) {
				 * 
				 * 
				 * Printing the RDD's as JSON Object
				 * 
				 * 
				 * Object json = mapper.readValue(ls.get(i), Object.class);
				 * mapper.configure(SerializationFeature.INDENT_OUTPUT, true);
				 * //System.out.println(mapper.writerWithDefaultPrettyPrinter().
				 * writeValueAsString(json)); }
				 */
				return null;
			}
		});
	}

	/*
	 * public static void checkUpdate(String string) throws JSONException {
	 * 
	 * 1. Convert string to json 2. extract the name. 3. check if the name is in
	 * HashMap. 3.1 if not --> create new object(Lighting class) of that name
	 * and initialize with default values. 3.2 if yes --> go to step 4. 4. check
	 * the state of light with initialState of light object. 4.1 If state has
	 * not changed --> do nothing. 4.2 If state has changed --> Go to step 5. 5.
	 * Check for the following cases: 5.1 If state changed from Red --> Green
	 * update initialState, and store the timeStamp 5.2 If state changed from
	 * Green --> Red update timestamp to new timestamp, update Ontime for light,
	 * update initialState.
	 * 
	 * 
	 * System.out.println(string); JSONObject jobj = new JSONObject(string);
	 * String name = jobj.getString("name"); String currentState =
	 * jobj.getString("state"); Timestamp timestamp =
	 * Timestamp.valueOf(jobj.getString("Timestamp")); Calendar temp =
	 * Calendar.getInstance(); temp.set(Calendar.HOUR, 0);
	 * temp.set(Calendar.MINUTE, 0); temp.set(Calendar.SECOND, 0);
	 * temp.set(Calendar.MILLISECOND, 0); long onTime = temp.getTimeInMillis();
	 * if (!objectHashmap.containsKey(name)) { Lighting light = new Lighting();
	 * light.setName(name); light.setOnTime(onTime);
	 * light.setTimestamp(timestamp); light.setIntialState(currentState);
	 * objectHashmap.put(name, light); System.out.println("Light added: "
	 * +light.getName()+" with onTime: "+light.getOnTime()); } else { Lighting
	 * light = objectHashmap.get(name); String initialState =
	 * light.getIntialState(); if (!initialState.equals(currentState)) { if
	 * (initialState.equals("Red") && currentState.equals("Green")) {
	 * 
	 * 
	 * State changed from Red to Green update initialState, and store the
	 * timeStamp
	 * 
	 * 
	 * System.out.println("State Changed from Red to Green");
	 * light.setTimestamp(timestamp); light.setIntialState(currentState);
	 * 
	 * } else if (initialState.equals("Green") && currentState.equals("Red")) {
	 * 
	 * 
	 * State changed from Green to Red update timestamp to new timestamp, update
	 * Ontime for light, update initialState.
	 * 
	 * 
	 * // The total time for which the light was on. System.out.println(
	 * "State Changed from Green to Red"); long diff =
	 * light.getTimestamp().getTime() - timestamp.getTime(); long oldTimeInMilli
	 * = light.getOnTime(); oldTimeInMilli = oldTimeInMilli + diff;
	 * 
	 * light.setOnTime(oldTimeInMilli); System.out.println(
	 * "New Updated onTime for "+light.getName()+" is "+light.getOnTime());
	 * light.setTimestamp(timestamp); light.setIntialState(currentState); }
	 * 
	 * } } }
	 */

}