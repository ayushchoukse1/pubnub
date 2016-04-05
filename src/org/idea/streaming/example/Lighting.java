package org.idea.streaming.example;

import java.sql.Timestamp;
import java.util.Calendar;

public class Lighting {

	private Timestamp timestamp;
	private Timestamp onTime;
	private String intialState;
	private String name;

	public Lighting(Timestamp timestamp, Timestamp onTime, String intialState, String name) {
		super();
		this.timestamp = timestamp;
		this.onTime = onTime;
		this.intialState = intialState;
		this.name = name;
	}

	
	public Lighting() {
		
	}


	public Timestamp getTimestamp() {
		return timestamp;
	}

	public void setTimestamp(Timestamp timestamp) {
		this.timestamp = timestamp;
	}

	public Timestamp getOnTime() {
		return onTime;
	}

	public void setOnTime(Timestamp onTime) {
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
