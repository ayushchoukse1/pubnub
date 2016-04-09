package org.idea.streaming.example;

import java.util.ArrayList;

import java.util.List;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.fusesource.mqtt.client.BlockingConnection;
import org.fusesource.mqtt.client.MQTT;
import org.fusesource.mqtt.client.Message;
import org.fusesource.mqtt.client.QoS;
import org.fusesource.mqtt.client.Topic;
import com.pubnub.api.Pubnub;
import com.pubnub.api.Callback;
import com.pubnub.api.PubnubError;

public class Pubnub1 implements Runnable {
	private static final String MQTT_BROKER_TOPICS = "mqttbrokertopics";
	private static final String MQTT_BROKER_PORT = "mqttbrokerport";
	private static final String MQTT_BROKER_HOST = "mqttbrokerhost";
	private static final String SERIALIZER_CLASS = "serializerclass";
	private static final String BROKER_LIST = "brokerlist";
	private static final String[] args = null;

	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		Pubnub1 pub = new Pubnub1();
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
			Pubnub pubnub = new Pubnub("pub-c-96f9cabb-5663-43c5-a726-44a25484873c","sub-c-829abab8-fc6d-11e5-8cfb-0619f8945a4f");
			CommandLineParser parser = new PosixParser();
			CommandLine cmd;
			Options options = new Options();
				cmd = parser.parse(options, args);
			MQTT mqtt = new MQTT();
			mqtt.setHost(cmd.getOptionValue(MQTT_BROKER_HOST, "whipple.dyndns-home.com"),
					Integer.parseInt(cmd.getOptionValue(MQTT_BROKER_PORT, "1883")));

			BlockingConnection connection = mqtt.blockingConnection();
			connection.connect();

			String topicsArg = cmd.getOptionValue(MQTT_BROKER_TOPICS, "topic");
			List<Topic> topicsList = new ArrayList<Topic>();
			String[] topics = topicsArg.split(",");
			for (String topic : topics) {
				topicsList.add(new Topic(topic, QoS.AT_LEAST_ONCE));
			}

			Topic[] mqttTopics = topicsList.toArray(new Topic[] {});
			byte[] qoses = connection.subscribe(mqttTopics);

			boolean exit = false;
			int sec = 0;
			
			Callback callback = new Callback() {
				public void successCallback(String channel, Object response) {
					System.out.println(response.toString());
				}

				public void errorCallback(String channel, PubnubError error) {
					System.out.println(error.toString());
				}
			};
			do {
				Message message = connection.receive();
				byte[] payload = message.getPayload();
				String strPayload = new String(payload);
				
				message.ack();
				JSONObject jobj;
				try {
					jobj = new JSONObject(strPayload);
				} catch (JSONException e) {
					// TODO: handle exception
					String str = "{\"JSONExceptoion\":\""+strPayload+"\"}";
					jobj = new JSONObject(str);
				}
				/*
				 * Adding timestamp to the incoming messages by converting them
				 * to Json and appending new field at the end.
				 */
				System.out.println(jobj.toString());
				pubnub.publish("pubnub_topic", jobj.toString() , callback);
			}while(!exit);
			 
		} catch (Exception e) {
			// TODO: handle exception
		}
	}

}
