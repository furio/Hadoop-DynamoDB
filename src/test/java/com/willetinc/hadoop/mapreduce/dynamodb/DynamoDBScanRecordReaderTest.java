package com.willetinc.hadoop.mapreduce.dynamodb;

import static org.easymock.EasyMock.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

import com.amazonaws.services.dynamodb.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodb.model.AttributeValue;
import com.amazonaws.services.dynamodb.model.Key;
import com.amazonaws.services.dynamodb.model.ScanRequest;
import com.amazonaws.services.dynamodb.model.ScanResult;
import com.willetinc.hadoop.mapreduce.dynamodb.DynamoDBConfiguration;
import com.willetinc.hadoop.mapreduce.dynamodb.DynamoDBRecordReader;
import com.willetinc.hadoop.mapreduce.dynamodb.DynamoDBScanRecordReader;
import com.willetinc.hadoop.mapreduce.dynamodb.DynamoDBInputFormat.DynamoDBInputSplit;

public class DynamoDBScanRecordReaderTest {
	
	@Test
	public void testRecordReader() throws IOException, InterruptedException {
		final String TABLE_NAME = "clickstream-log";
		final String ACCESS_KEY = "access_key";
		final String SECRET_KEY = "secret_key";

		Configuration conf = new Configuration();
		DynamoDBConfiguration.configureDB(conf, TABLE_NAME, ACCESS_KEY,
				SECRET_KEY);

		DynamoDBConfiguration dbConf = new DynamoDBConfiguration(conf);

		DynamoDBInputSplit inputSplit = createStrictMock(DynamoDBInputSplit.class);
		AmazonDynamoDBClient client = createStrictMock(AmazonDynamoDBClient.class);
		
		ScanResult result = createStrictMock(ScanResult.class);
		Key lastKey = createStrictMock(Key.class);
		List<Map<String, AttributeValue>> list = new ArrayList<Map<String, AttributeValue>>();
		Map<String, AttributeValue> value = new HashMap<String, AttributeValue>();
		value.put("storeid", new AttributeValue().withN("22"));
		value.put("timestamp", new AttributeValue().withN("1353123945999"));
		value.put("clickstream", new AttributeValue().withS("673 713"));
		list.add(value);
		
		// first set of results
		expect(client.scan(anyObject(ScanRequest.class))).andReturn(result);
		expect(result.getLastEvaluatedKey()).andReturn(lastKey);
		expect(result.getItems()).andReturn(list);
		
		// second set of results
		expect(client.scan(anyObject(ScanRequest.class))).andReturn(result);
		expect(result.getLastEvaluatedKey()).andReturn(null);
		expect(result.getItems()).andReturn(list);

		replay(inputSplit);
		replay(client);
		replay(result);
		replay(lastKey);
		
		DynamoDBRecordReader<CSRecord> reader = new DynamoDBScanRecordReader<CSRecord>(
				inputSplit,
				CSRecord.class, 
				conf, 
				client, 
				dbConf, 
				null, 
				TABLE_NAME);
		
		// first set of results
		assertTrue(reader.nextKeyValue());
		assertEquals(0, reader.getCurrentKey().get());
		
		// second set of results
		assertTrue(reader.nextKeyValue());
		assertEquals(1, reader.getCurrentKey().get());
		
		// no more results
		assertFalse(reader.nextKeyValue());
		
		verify(inputSplit);
		verify(client);
		verify(result);
		verify(lastKey);
	}

}
