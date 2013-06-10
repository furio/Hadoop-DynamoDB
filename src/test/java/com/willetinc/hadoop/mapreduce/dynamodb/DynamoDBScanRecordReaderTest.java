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
import org.junit.Test;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ScanRequest;
import com.amazonaws.services.dynamodbv2.model.ScanResult;

public class DynamoDBScanRecordReaderTest {
	
	@Test
	public void testRecordReader() throws IOException, InterruptedException {
		final String TABLE_NAME = "clickstream-log";
		final String ACCESS_KEY = "access_key";
		final String SECRET_KEY = "secret_key";

		Configuration conf = new Configuration();
		DynamoDBConfiguration.setCredentals(conf, ACCESS_KEY, SECRET_KEY);
		DynamoDBConfiguration dbConf = new DynamoDBConfiguration(conf);

		DynamoDBScanInputSplit inputSplit = createStrictMock(DynamoDBScanInputSplit.class);
		AmazonDynamoDBClient client = createStrictMock(AmazonDynamoDBClient.class);
		
		ScanResult result = createStrictMock(ScanResult.class);
		@SuppressWarnings("unchecked")
		Map<String, AttributeValue> lastKey = createStrictMock(Map.class);
		List<Map<String, AttributeValue>> list = new ArrayList<Map<String, AttributeValue>>();
		Map<String, AttributeValue> value = new HashMap<String, AttributeValue>();
		value.put("storeid", new AttributeValue().withN("22"));
		value.put("timestamp", new AttributeValue().withN("1353123945999"));
		value.put("clickstream", new AttributeValue().withS("673 713"));
		list.add(value);
		
		// first set of results
		expect(inputSplit.getTotalSegments()).andReturn(2);
		expect(inputSplit.getSegment()).andReturn(1);
		expect(client.scan(anyObject(ScanRequest.class))).andReturn(result);
		expect(result.getLastEvaluatedKey()).andReturn(lastKey);
		expect(result.getItems()).andReturn(list);
		
		// second set of results
		expect(inputSplit.getTotalSegments()).andReturn(2);
		expect(inputSplit.getSegment()).andReturn(1);
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
