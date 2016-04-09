package org.idea.streaming.example;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;
import org.codehaus.jettison.json.JSONObject;

import com.pubnub.api.Callback;
import com.pubnub.api.Pubnub;
import com.pubnub.api.PubnubError;

import scala.util.Random;

public class PubnubTest implements Runnable {
	
	private static final String[] args = null;
	private static Pubnub pubnub = new Pubnub("pub-c-96f9cabb-5663-43c5-a726-44a25484873c","sub-c-829abab8-fc6d-11e5-8cfb-0619f8945a4f");
	String[] type1 = {
			"{\"client\":\"I.D.E.A. Lighting\",\"command\":\"Lighting State\",\"name\":\"Family Couch E\",\"state\":\"Red\"}", 
			"{\"client\":\"I.D.E.A. Lighting\",\"command\":\"Lighting State\",\"name\":\"Aspen\",\"state\":\"Green\"}", 
			"{\"client\":\"I.D.E.A. Lighting\",\"command\":\"Lighting State\",\"name\":\"Family W\",\"state\":\"Red\"}",
			"{\"client\":\"I.D.E.A. Lighting\",\"command\":\"Lighting State\",\"name\":\"Family TV\",\"state\":\"Green\"}",
			"{\"client\":\"I.D.E.A. Lighting\",\"command\":\"Lighting State\",\"name\":\"Family Couch W\",\"state\":\"Red\"}",
			"{\"client\":\"I.D.E.A. Lighting\",\"command\":\"Lighting State\",\"name\":\"Round Room E\",\"state\":\"Green\"}",
			"{\"client\":\"I.D.E.A. Lighting\",\"command\":\"Lighting State\",\"name\":\"Round Room NW\",\"state\":\"Red\"}",
			"{\"client\":\"I.D.E.A. Lighting\",\"command\":\"Lighting State\",\"name\":\"Round Room SW\",\"state\":\"Green\"}",
			"{\"client\":\"I.D.E.A. Lighting\",\"command\":\"Lighting State\",\"name\":\"Family E\",\"state\":\"Red\"}",
			"{\"client\":\"I.D.E.A. Lighting\",\"command\":\"Lighting State\",\"name\":\"Horse Picture\",\"state\":\"Green\"}",
			"{\"deviceId\":\"28:db:b1:1f:06:00:00:d3\",\"deviceType\":\"DS18B20\",\"celciusTemperature\":"+(new Random().nextInt((50 - 5) + 1) + 5)+",\"temperature\":75.43}",
			"{\"deviceId\":\"28:26:1c:60:07:00:00:ad\",\"deviceType\":\"DS18B20\",\"celciusTemperature\":"+(new Random().nextInt((50 - 5) + 10) + 5)+",\"temperature\":75.43}",
			"{\"deviceId\":\"28:ff:2c:31:44:04:00:c2\",\"deviceType\":\"DS18B20\",\"celciusTemperature\":"+(new Random().nextInt((50 - 5) + 15) + 5)+",\"temperature\":75.43}",
	};
String[] type2 = {
	"{\"client\":\"I.D.E.A. Lighting\",\"command\":\"Lighting State\",\"name\":\"Family Couch E\",\"state\":\"Green\"}", 
	"{\"client\":\"I.D.E.A. Lighting\",\"command\":\"Lighting State\",\"name\":\"Aspen\",\"state\":\"Red\"}", 
	"{\"client\":\"I.D.E.A. Lighting\",\"command\":\"Lighting State\",\"name\":\"Family W\",\"state\":\"Green\"}",
	"{\"client\":\"I.D.E.A. Lighting\",\"command\":\"Lighting State\",\"name\":\"Family TV\",\"state\":\"Red\"}",
	"{\"client\":\"I.D.E.A. Lighting\",\"command\":\"Lighting State\",\"name\":\"Family Couch W\",\"state\":\"Green\"}",
	"{\"client\":\"I.D.E.A. Lighting\",\"command\":\"Lighting State\",\"name\":\"Round Room E\",\"state\":\"Red\"}",
	"{\"client\":\"I.D.E.A. Lighting\",\"command\":\"Lighting State\",\"name\":\"Round Room NW \",\"state\":\"Green\"}",
	"{\"client\":\"I.D.E.A. Lighting\",\"command\":\"Lighting State\",\"name\":\"Round Room SW\",\"state\":\"Red\"}",
	"{\"client\":\"I.D.E.A. Lighting\",\"command\":\"Lighting State\",\"name\":\"Family E\",\"state\":\"Green\"}",
	"{\"client\":\"I.D.E.A. Lighting\",\"command\":\"Lighting State\",\"name\":\"Horse Picture\",\"state\":\"Red\"}",
	"{\"deviceId\":\"28:db:b1:1f:06:00:00:d3\",\"deviceType\":\"DS18B20\",\"celciusTemperature\":"+(new Random().nextInt((50 - 5) + 1) + 5)+",\"temperature\":75.43}",
	"{\"deviceId\":\"28:26:1c:60:07:00:00:ad\",\"deviceType\":\"DS18B20\",\"celciusTemperature\":"+(new Random().nextInt((50 - 5) + 1) + 5)+",\"temperature\":75.43}",
	"{\"deviceId\":\"28:ff:2c:31:44:04:00:c2\",\"deviceType\":\"DS18B20\",\"celciusTemperature\":"+(new Random().nextInt((50 - 5) + 1) + 5)+",\"temperature\":75.43}",
};
	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		PubnubTest pub = new PubnubTest();
		pub.start();
	}
	public void start() {
		Thread t = new Thread(this);
		t.start();
	}
	@Override
	public void run() {
		// TODO Auto-generated method stub
		try {
			//System.out.println("run");
			Callback callback = new Callback() {
				public void successCallback(String channel, Object response) {
					System.out.println("success"+response.toString());
				}

				public void errorCallback(String channel, PubnubError error) {
					System.out.println("error "+error.toString());
				}
			};
			
				printRed(type1,type2,callback);
				//JSONObject jobj;
				
				/*
				 * Adding timestamp to the incoming messages by converting them
				 * to Json and appending new field at the end.
				 */
				//pubnub.publish("pubnub_test_topic", jobj.toString() , callback);
			
			 
		} catch (Exception e) {
			// TODO: handle exception
		}
	}
	public static void printRed( String[] type1,String[] type2,Callback callback) throws Exception {
		long startTimeRed = System.nanoTime();
		System.out.println("Type1");
		//while ((System.nanoTime() - startTimeRed) < 0.5 * 60 * NANO) {

			//System.out.println((System.nanoTime() - startTimeRed) + "  < " + 0.5 * 60 * NANO);
			for (int i = 0; i < type1.length; i++) {
				Thread.sleep(500);
				JSONObject jobj = new JSONObject(type1[i]);
				System.out.println(jobj.toString());
				String str1 ="{\"deviceId\":\"28:db:b1:1f:06:00:00:d3\",\"deviceType\":\"DS18B20\",\"celciusTemperature\":"+(new Random().nextInt((50 - 5) + 1) + 5)+",\"temperature\":75.43}";
				String str2 = "{\"deviceId\":\"28:26:1c:60:07:00:00:ad\",\"deviceType\":\"DS18B20\",\"celciusTemperature\":"+(new Random().nextInt((50 - 5) + 1) + 5)+",\"temperature\":75.43}";
				String str3 = "{\"deviceId\":\"28:ff:2c:31:44:04:00:c2\",\"deviceType\":\"DS18B20\",\"celciusTemperature\":"+(new Random().nextInt((50 - 5) + 1) + 5)+",\"temperature\":75.43}";
				pubnub.publish("pubnub_test_topic", jobj.toString() , callback);
				Thread.sleep(500);
				pubnub.publish("pubnub_test_topic", str1 , callback);
				Thread.sleep(500);
				pubnub.publish("pubnub_test_topic", str2 , callback);
				Thread.sleep(500);
				pubnub.publish("pubnub_test_topic", str3 , callback);
			}
			
		//}
		printGreen(type2,type1,callback);
	}

	public static void printGreen(String[] type2,String[] type1, Callback callback) throws Exception {
		long startTimeGreen = System.nanoTime();
		System.out.println("Type2");
	//	while ((System.nanoTime() - startTimeGreen) < 0.5 * 60 * NANO_GREEN) {
			//System.out.println((System.nanoTime() - startTimeGreen) + "  < " + 0.5 * 60 * NANO);
			for (int i = 0; i < type2.length; i++) {
				Thread.sleep(500);
				JSONObject jobj = new JSONObject(type2[i]);
				System.out.println(jobj.toString());
				String str1 ="{\"deviceId\":\"28:db:b1:1f:06:00:00:d3\",\"deviceType\":\"DS18B20\",\"celciusTemperature\":"+(new Random().nextInt((50 - 5) + 1) + 5)+",\"temperature\":75.43}";
				String str2 = "{\"deviceId\":\"28:26:1c:60:07:00:00:ad\",\"deviceType\":\"DS18B20\",\"celciusTemperature\":"+(new Random().nextInt((50 - 5) + 1) + 5)+",\"temperature\":75.43}";
				String str3 = "{\"deviceId\":\"28:ff:2c:31:44:04:00:c2\",\"deviceType\":\"DS18B20\",\"celciusTemperature\":"+(new Random().nextInt((50 - 5) + 1) + 5)+",\"temperature\":75.43}";
				pubnub.publish("pubnub_test_topic", jobj.toString() , callback);
				Thread.sleep(500);
				pubnub.publish("pubnub_test_topic", str1 , callback);
				Thread.sleep(500);
				pubnub.publish("pubnub_test_topic", str2 , callback);
				Thread.sleep(500);
				pubnub.publish("pubnub_test_topic", str3 , callback);
				
			}
			
		//}
		printRed(type1,type2,callback);
	}


}
