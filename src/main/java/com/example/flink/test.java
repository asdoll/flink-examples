package com.example.flink;

import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class test {

	public static void main(String[] args) throws InterruptedException {
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		Float a = (float)2;
		System.out.println(a.equals(2));
	}
	
}
