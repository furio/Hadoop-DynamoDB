package com.willetinc.hadoop.mapreduce.dynamodb;

public class CSRecord extends DynamoDBItemWritable {
	
	public CSRecord() {
		super(
			new NWritable("store_id") {},
			new NWritable("timestamp") {},
			new SWritable("clickstream") {});
	}
}
