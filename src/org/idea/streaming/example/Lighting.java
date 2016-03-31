package org.idea.streaming.example;

import java.sql.Time;
import java.sql.Timestamp;

public class Lighting {

	private Timestamp timestamp;
	private Time onTime;
	private String intialState;
	private String name;

	public Lighting(Timestamp timestamp, Time onTime, String intialState, String name) {
		super();
		this.timestamp = timestamp;
		this.onTime = onTime;
		this.intialState = intialState;
		this.name = name;
	}

	public Timestamp getTimestamp() {
		return timestamp;
	}

	public void setTimestamp(Timestamp timestamp) {
		this.timestamp = timestamp;
	}

	public Time getOnTime() {
		return onTime;
	}

	public void setOnTime(Time onTime) {
		this.onTime = onTime;
	}

	public String getIntialState() {
		return intialState;
	}

	public void setIntialState(String intialState) {
		this.intialState = intialState;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public void updateState() {

	}

	public void updateOnTime() {

	}

}
