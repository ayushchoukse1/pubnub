package org.idea.streaming.example;

import java.util.concurrent.TimeUnit;

public class Delete {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		long millis = 86440;
		System.out.println(String.format("%02d:%02d:%02d", TimeUnit.MILLISECONDS.toHours(millis),
				TimeUnit.MILLISECONDS.toMinutes(millis)
						- TimeUnit.HOURS.toMinutes(TimeUnit.MILLISECONDS.toHours(millis)),
				TimeUnit.MILLISECONDS.toSeconds(millis)
						- TimeUnit.MINUTES.toSeconds(TimeUnit.MILLISECONDS.toMinutes(millis))));
	}

}
