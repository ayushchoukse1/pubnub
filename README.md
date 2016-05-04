# PUBNUB HOME AUTOMATION 
###### - AYUSH CHOUKSE  ayush.choukse@sjsu.edu

Link: [Dashboard](http://goo.gl/TmJHp5)

Youtube: [Youtube Demo video](https://youtu.be/AD7lSYy3JEA)

Sample Home Automation Application demonstrating use of Java APIs and Javascript APIs of Pubnub.
```
Publish Key-  	“pub-c-96f9cabb-5663-43c5-a726-44a25484873c”
Subscribe Key- 	“sub-c-829abab8-fc6d-11e5-8cfb-0619f8945a4f”
Channels- 
    a.  “pubnub_topic” –          Actual real-time stream channel.
    b.  “pubnub_test_topic” –     Test streams to show working of application.
    c.  “c3-spline” –             Channel to publish thermostat data for Eon chart.
```
Aim: To demonstrate the use of Pubnub’s API in an application.

Description: 

I have built two services and the description of each service has been given below.

**1.	Pubnub Real-Time Publishing-**

I have used **Pubnub’s Java SDK** to implement the module. This is application publishes the real-time input stream of home automation that my app is receiving from an MQTT source. There are two kinds of streams that I am receiving for the scope of this demonstration. First stream has information about the lights installed and its state. A state of light can either be `Green` (Turned on) or `Red` (Turned off). I am displaying this information on under the lights tab. The second stream is data coming from thermostat which sends the current temperature in celsiusTemperature which is always an integer.
```
    Sample light data –
     {"client":"I.D.E.A. Lighting","command":"Lighting State","name":"Round Room SW","state":"Red"} 
    	SCHEMA
    { 
    client: I.D.E.A. Lighting,
    command: Lighting State,
    name: Round Room SW,
    state: Red
     }
    Sample temperature data- 
    {"deviceId":"28:26:1c:60:07:00:00:ad","deviceType":"DS18B20","celciusTemperature":13.38,"temperature":56.08} 
    
    SCHEMA
    { 
    deviceId: 28:26:1c:60:07:00:00:ad,
    deviceType: DS18B20,
    celciusTemperature: 13.38,
    temperature: 56.08
     }
```

I have written two classes to generate data. 

a. `Pubnub1.java` - This class is publishing actual real-time data from the MQTT sources directly to the `pubnub_topic`.

* Pubnub API used – **pubnub.publish().**

b. `PubnubTest.java` – This class is publishing sample test data to `pubnub_test_topic`.

* Pubnub API used – **pubnub.publish().**

```Note: source code for both of the files can be found in ‘pubnub/src/org/idea/streaming/example/’ folder.```

**2.   Pubnub real-time dashboard-**

  I have used **Pubnub's Javascript SDK**. This module has two web pages realtime-dashboard.html and test-dashboard.html that are displaying the information coming from `pubnub_topic` and `pubnub_test_topic`channels.

  Pubnub APIs used – 
* **Pubnub.init()** – To initialize instance of Pubnub with publish key and subscribe key.
* **Pubnub.subscribe()** – To subscribe the channels on which streaming data is published.
* **Eon.chart()**- To display information of thermostat data in real-time. This chart displays the changes in temperature every second on a `spline` chart by subscribing to `c3-spline` channel.
* **Pubnub.publish()**- To publish the thermostat data coming from subscribed channels to eon chart using `c3-spline` channel.


