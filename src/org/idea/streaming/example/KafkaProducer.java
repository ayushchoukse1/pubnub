package org.idea.streaming.example;

import java.util.Properties;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

public class KafkaProducer {

	private static Producer<String, String> producer;
	public String topic = "topic";
	
	/**
	 * To initialize the producer, you need to define required properties. Properties are wrapped in ProducerConfig object.
	 * Producer is initialized using ProducerConfig. On initializing Producer, connection is established between Producer and Kafka Broker. 
	 * Broker is running on 'localhost:9092'
	 */
	public void initialize(String topic){
		setTopic(topic);
		Properties producerProps = new Properties();
		producerProps.put("metadata.broker.list", "localhost:9092");
		producerProps.put("serializer.class", "kafka.serializer.StringEncoder");
		producerProps.put("request.required.acks", "1");
		ProducerConfig producerConfig = new ProducerConfig(producerProps);
		producer = new Producer<String, String>(producerConfig);
	}
	
	
	/**
	 * In this method, string message is read from program passed as parameter.
	 * KeyedMessage is initialized with topic name and message. Now, producers send() API is used to send message by passing
	 * KeyedMessage reference. Based on topic name in KeyedMessage, producer sends message to corresponding topic.
	 * @param data
	 * @throws Exception
	 */
	public void publishMessage(String messagePayload) throws Exception{
		KeyedMessage<String, String> keyedMessage = new KeyedMessage<String, String>(getTopic(), messagePayload);
		producer.send(keyedMessage);
	}

	public void closeConnection(){
		try {
			producer.close();
		} catch (Exception e) {

		}
	}


	public String getTopic() {
		return topic;
	}


	public void setTopic(String topic) {
		topic = topic;
	}
	
}
