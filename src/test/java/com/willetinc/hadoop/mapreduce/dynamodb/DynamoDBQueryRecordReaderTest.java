/**
 * Copyright 2012 Willet Inc.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
import org.apache.hadoop.mapreduce.Job;
import org.junit.Test;

import com.amazonaws.services.dynamodb.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodb.model.AttributeValue;
import com.amazonaws.services.dynamodb.model.ComparisonOperator;
import com.amazonaws.services.dynamodb.model.Key;
import com.amazonaws.services.dynamodb.model.QueryRequest;
import com.amazonaws.services.dynamodb.model.QueryResult;
import com.willetinc.hadoop.mapreduce.dynamodb.DynamoDBConfiguration;
import com.willetinc.hadoop.mapreduce.dynamodb.DynamoDBQueryRecordReader;
import com.willetinc.hadoop.mapreduce.dynamodb.DynamoDBRecordReader;
import com.willetinc.hadoop.mapreduce.dynamodb.DynamoDBQueryInputFormat.DynamoDBQueryInputSplit;

public class DynamoDBQueryRecordReaderTest {
	
	private static final String TABLE_NAME = "clickstream-log";
	private static final String ACCESS_KEY = "access_key";
	private static final String SECRET_KEY = "secret_key";
	
	@Test
	public void testConstructor() throws IOException {
		Job job = new Job();
		Configuration conf = job.getConfiguration();
		DynamoDBConfiguration.setCredentals(conf, ACCESS_KEY, SECRET_KEY);
		DynamoDBConfiguration dbConf = new DynamoDBConfiguration(conf);
		
		DynamoDBQueryInputSplit inputSplit = createStrictMock(DynamoDBQueryInputSplit.class);
		AmazonDynamoDBClient client = createStrictMock(AmazonDynamoDBClient.class);
		AttributeValue hashKeyValue = new AttributeValue().withN("007");
		
		// the important thing we are testing here is that the range key condition
		// is not specified when the rangeKeyValues collection is empty
		expect(inputSplit.getHashKeyValue()).andReturn(hashKeyValue);
		expect(inputSplit.hasRangeKey()).andReturn(false);
		
		replay(inputSplit);
		replay(client);
		
		new DynamoDBQueryRecordReader<CSRecord>(
				inputSplit,
				CSRecord.class, 
				conf, 
				client, 
				dbConf, 
				TABLE_NAME);
		
		verify(inputSplit);
		verify(client);
	}
	
	@Test
	public void testConstructorWithRangeKey() throws IOException {
		Job job = new Job();
		Configuration conf = job.getConfiguration();
		DynamoDBConfiguration.setCredentals(conf, ACCESS_KEY, SECRET_KEY);
		DynamoDBConfiguration dbConf = new DynamoDBConfiguration(conf);
		
		DynamoDBQueryInputSplit inputSplit = createStrictMock(DynamoDBQueryInputSplit.class);
		AmazonDynamoDBClient client = createStrictMock(AmazonDynamoDBClient.class);
		
		AttributeValue hashKeyValue = new AttributeValue().withN("007");
		List<AttributeValue> rangeKeyValues = new ArrayList<AttributeValue>();
		rangeKeyValues.add(new AttributeValue().withN("1353121913437"));
		
		// the important thing we are testing here is that the range key condition
		// is specified when the rangeKey operator and values are specified
		expect(inputSplit.getHashKeyValue()).andReturn(hashKeyValue);
		expect(inputSplit.hasRangeKey()).andReturn(true);
		expect(inputSplit.getRangeKeyOperator()).andReturn(ComparisonOperator.EQ);
		expect(inputSplit.getRangeKeyValues()).andReturn(rangeKeyValues);
		
		replay(inputSplit);
		replay(client);
		
		new DynamoDBQueryRecordReader<CSRecord>(
				inputSplit,
				CSRecord.class, 
				conf, 
				client, 
				dbConf, 
				TABLE_NAME);
		
		verify(inputSplit);
		verify(client);
	}
	
	@Test
	public void testRecordReader() throws IOException, InterruptedException {

		Job job = new Job();
		Configuration conf = job.getConfiguration();
		DynamoDBConfiguration.setCredentals(conf, ACCESS_KEY, SECRET_KEY);

		DynamoDBConfiguration dbConf = new DynamoDBConfiguration(conf);

		DynamoDBQueryInputSplit inputSplit = createStrictMock(DynamoDBQueryInputSplit.class);
		AttributeValue hashKeyValue = new AttributeValue().withN("007");
		AmazonDynamoDBClient client = createStrictMock(AmazonDynamoDBClient.class);
		
		QueryResult result = createStrictMock(QueryResult.class);
		Key lastKey = createStrictMock(Key.class);
		List<Map<String, AttributeValue>> list = new ArrayList<Map<String, AttributeValue>>();
		
		Map<String, AttributeValue> value = new HashMap<String, AttributeValue>();
		value.put("storeid", new AttributeValue().withN("22"));
		value.put("timestamp", new AttributeValue().withN("1353123945999"));
		value.put("clickstream", new AttributeValue().withS("673 713"));
		list.add(value);
		
		// first set of results
		expect(inputSplit.getHashKeyValue()).andReturn(hashKeyValue);
		expect(inputSplit.hasRangeKey()).andReturn(false);
		expect(client.query(anyObject(QueryRequest.class))).andReturn(result);
		expect(result.getLastEvaluatedKey()).andReturn(lastKey);
		expect(result.getItems()).andReturn(list);
		
		// second set of results
		expect(client.query(anyObject(QueryRequest.class))).andReturn(result);
		expect(result.getLastEvaluatedKey()).andReturn(null);
		expect(result.getItems()).andReturn(list);

		replay(inputSplit);
		replay(client);
		replay(result);
		replay(lastKey);
		
		DynamoDBRecordReader<CSRecord> reader = new DynamoDBQueryRecordReader<CSRecord>(
				inputSplit,
				CSRecord.class, 
				conf, 
				client, 
				dbConf, 
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
