package org.idea.streaming.example;

import java.io.PrintWriter;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.Properties;
import java.util.Timer;
import java.util.TimerTask;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.PosixParser;
import org.codehaus.jettison.json.JSONObject;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

public class TestProducer implements Runnable{
	private static final String SERIALIZER_CLASS = "serializerclass";
	private static final String BROKER_LIST = "brokerlist";
	static long NANO = 10001 * 1000 * 1000;
	static long NANO_GREEN = 10001 * 1000 * 1000;

	/*public static void main(String[] args) throws Exception {
		KafkaProducer kafkaProducer = new KafkaProducer();
		kafkaProducer.initialize("topic");
		String Red = "{\"client\":\"I.D.E.A. Lighting\",\"command\":\"Lighting State\",\"name\":\"Family Couch E\",\"state\":\"Red\"}";
		String Green = "{\"client\":\"I.D.E.A. Lighting\",\"command\":\"Lighting State\",\"name\":\"Family Couch E\",\"state\":\"Green\"}";
		Timer timer = new Timer();
		printRed(kafkaProducer, Red);
		printGreen(kafkaProducer, Green);
		printRed(kafkaProducer, Red);
		printGreen(kafkaProducer, Green);

	}
*/
	public void start() {
		Thread t = new Thread(this);
		t.start();
	}
	
	public static void printRed(KafkaProducer kafkaProducer, String Red) throws Exception {
		long startTimeRed = System.nanoTime();
		
		while ((System.nanoTime() - startTimeRed) < 0.5 * 60 * NANO) {

			//System.out.println((System.nanoTime() - startTimeRed) + "  < " + 0.5 * 60 * NANO);
			JSONObject jobj = new JSONObject(Red);
			Timestamp originalTimeStamp = new Timestamp(System.currentTimeMillis());
			Calendar calender = Calendar.getInstance();
			calender.setTimeInMillis(originalTimeStamp.getTime());
			jobj.put("TimeStamp", new Timestamp(calender.getTime().getTime()));
			kafkaProducer.publishMessage(jobj.toString());
			Thread.sleep(1000);

		}
	}

	public static void printGreen(KafkaProducer kafkaProducer, String Green) throws Exception {
		long startTimeGreen = System.nanoTime();
		
		while ((System.nanoTime() - startTimeGreen) < 0.5 * 60 * NANO_GREEN) {
			//System.out.println((System.nanoTime() - startTimeGreen) + "  < " + 0.5 * 60 * NANO);
			JSONObject jobj = new JSONObject(Green);
			Timestamp originalTimeStamp = new Timestamp(System.currentTimeMillis());
			Calendar calender = Calendar.getInstance();
			calender.setTimeInMillis(originalTimeStamp.getTime());
			jobj.put("TimeStamp", new Timestamp(calender.getTime().getTime()));
			kafkaProducer.publishMessage(jobj.toString());
			Thread.sleep(1000);
		}
	}

	@Override
	public void run() {
		// TODO Auto-generated method stub
		KafkaProducer kafkaProducer = new KafkaProducer();
		kafkaProducer.initialize("topic");
		String Red = "{\"client\":\"I.D.E.A. Lighting\",\"command\":\"Lighting State\",\"name\":\"Family Couch E\",\"state\":\"Red\"}";
		String Green = "{\"client\":\"I.D.E.A. Lighting\",\"command\":\"Lighting State\",\"name\":\"Family Couch E\",\"state\":\"Green\"}";
		Timer timer = new Timer();
		try {
			printRed(kafkaProducer, Red);
			printGreen(kafkaProducer, Green);
			printRed(kafkaProducer, Red);
			printGreen(kafkaProducer, Green);
			printRed(kafkaProducer, Red);
			printGreen(kafkaProducer, Green);
			printRed(kafkaProducer, Red);
			printGreen(kafkaProducer, Green);
			printRed(kafkaProducer, Red);
			printGreen(kafkaProducer, Green);
			printRed(kafkaProducer, Red);
			printGreen(kafkaProducer, Green);
			printRed(kafkaProducer, Red);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
}
