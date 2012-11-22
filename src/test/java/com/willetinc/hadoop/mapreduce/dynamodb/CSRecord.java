package com.willetinc.hadoop.mapreduce.dynamodb;

import java.util.Map;

import com.amazonaws.services.dynamodb.model.AttributeValue;
import com.willetinc.hadoop.mapreduce.dynamodb.DynamoDBItemWritable;
import com.willetinc.hadoop.mapreduce.dynamodb.Types;

public class CSRecord extends DynamoDBItemWritable {
	
	private AttributeValue clickstream;
	
	protected CSRecord() {
		super("storeid", Types.NUMBER, "timestamp", Types.NUMBER);
	}

	@Override
	public void doReadFields(Map<String, AttributeValue> in) {
		clickstream = in.get("clickstream");
	}
	
	public int getStoreId() {
		return Integer.parseInt(getHashKeyValue().getN());
	}

	public void setStoreId(int storeId) {
		getHashKeyValue().setN(Integer.toString(storeId));
	}

	public long getTimestamp() {
		return Long.parseLong(getRangeKeyValue().getN());
	}

	public void setTimestamp(long timestamp) {
		getRangeKeyValue().setN(Long.toString(timestamp));
	}

	public String getClickstream() {
		return clickstream.getS();
	}

	public void setClickstream(String clickstream) {
		this.clickstream.setS(clickstream);
	}

}
