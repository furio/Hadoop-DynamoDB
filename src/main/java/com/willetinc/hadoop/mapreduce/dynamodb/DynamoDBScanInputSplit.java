package com.willetinc.hadoop.mapreduce.dynamodb;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class DynamoDBScanInputSplit extends DynamoDBInputSplit {
	
	int totalSegments;
	int segment;
	
	public DynamoDBScanInputSplit() {
		totalSegments = 0;
		segment = 0;
	}
	
	public DynamoDBScanInputSplit(int totalSegments, int segment) {
		this.totalSegments = totalSegments;
		this.segment = segment;
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		totalSegments = in.readInt();
		segment = in.readInt();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(totalSegments);
		out.writeInt(segment);
	}
	
	public int getTotalSegments() {
		return totalSegments;
	}
	
	public int getSegment() {
		return segment;
	}
}
