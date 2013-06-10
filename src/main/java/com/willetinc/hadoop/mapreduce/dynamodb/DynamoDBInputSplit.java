package com.willetinc.hadoop.mapreduce.dynamodb;

import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;

public abstract class DynamoDBInputSplit extends InputSplit implements Writable {

	/**
	* Default Constructor
	*/
	public DynamoDBInputSplit() {}
	
	@Override
	public long getLength() throws IOException, InterruptedException {
		return 0;
	}
	
	@Override
	public String[] getLocations() throws IOException, InterruptedException {
		return new String[] {};
	}
}
