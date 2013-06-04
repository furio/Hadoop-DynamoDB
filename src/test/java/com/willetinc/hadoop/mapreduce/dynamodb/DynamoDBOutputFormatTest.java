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

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.easymock.Capture;
import org.junit.Test;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.PutItemRequest;
import com.amazonaws.services.dynamodbv2.model.PutItemResult;
import com.willetinc.hadoop.mapreduce.dynamodb.io.DynamoDBItemWritable;
import com.willetinc.hadoop.mapreduce.dynamodb.io.NWritable;

/**
 * 
 */
public class DynamoDBOutputFormatTest {

	private static final String TABLE_NAME = "clickstream-log";
	private static final String ACCESS_KEY = "access_key";
	private static final String SECRET_KEY = "secret_key";

	private static final String HASHKEY_FIELD = "hashkey";
	private static final String HASHKEY_VALUE = "007";
	private static final String RANGEKEY_FIELD = "rangekey";
	private static final String RANGEKEY_VALUE = "005";
	

	private class MyTable extends DynamoDBItemWritable {
		public MyTable() {
			super(new NWritable(HASHKEY_FIELD) {
			}, new NWritable(RANGEKEY_FIELD) {
			});
		}
	}

	@Test
	public void testDynamoDBRecordWriter()
			throws InstantiationException,
			IllegalAccessException,
			IOException,
			InterruptedException {

		AmazonDynamoDBClient client = createMock(AmazonDynamoDBClient.class);
		TaskAttemptContext context = createMock(TaskAttemptContext.class);
		DynamoDBOutputFormat<MyTable, NullWritable> outputFormat = new DynamoDBOutputFormat<MyTable, NullWritable>();

		RecordWriter<MyTable, NullWritable> writer = outputFormat.getRecordWriter(
				client,
				TABLE_NAME);

		Capture<PutItemRequest> putCapture = new Capture<PutItemRequest>();
		expect(client.putItem(capture(putCapture))).andReturn(
				new PutItemResult());
		client.shutdown();

		AttributeValue hashKey = new AttributeValue().withN(HASHKEY_VALUE);
		AttributeValue rangeKey = new AttributeValue().withN(RANGEKEY_VALUE);

		MyTable record = new MyTable();
		record.setHashKeyValue(hashKey);
		record.setRangeKeyValue(rangeKey);

		replay(client);
		replay(context);

		writer.write(record, NullWritable.get());
		writer.close(context);
		PutItemRequest put = putCapture.getValue();
		Map<String, AttributeValue> item = put.getItem();

		assertEquals(2, item.size());
		assertEquals(hashKey, item.get(HASHKEY_FIELD));
		assertEquals(rangeKey, item.get(RANGEKEY_FIELD));

		verify(client);
		verify(context);
	}

	@Test
	public void testGetRecordWriter() throws IOException, InterruptedException {
		TaskAttemptContext context = createMock(TaskAttemptContext.class);
		Configuration conf = createMock(Configuration.class);
		DynamoDBOutputFormat<MyTable, NullWritable> outputFormat = new DynamoDBOutputFormat<MyTable, NullWritable>();

		expect(context.getConfiguration()).andReturn(conf);
		expect(conf.get(DynamoDBConfiguration.ACCESS_KEY_PROPERTY)).andReturn(ACCESS_KEY);
		expect(conf.get(DynamoDBConfiguration.SECRET_KEY_PROPERTY)).andReturn(SECRET_KEY);
		expect(conf.get(DynamoDBConfiguration.OUTPUT_TABLE_NAME_PROPERTY)).andReturn(TABLE_NAME);
		expect(conf.get(DynamoDBConfiguration.DYNAMODB_ENDPOINT)).andReturn("test");
		
		replay(context);
		replay(conf);

		DynamoDBOutputFormat<MyTable, NullWritable>.DynamoDBRecordWriter writer = 
				(DynamoDBOutputFormat<MyTable, NullWritable>.DynamoDBRecordWriter) outputFormat.getRecordWriter(context);
		assertEquals(TABLE_NAME, writer.getTableName());

		verify(context);
		verify(conf);
	}
}
