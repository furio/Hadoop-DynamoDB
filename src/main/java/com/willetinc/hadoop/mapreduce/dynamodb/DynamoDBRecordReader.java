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

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.util.ReflectionUtils;

import com.amazonaws.services.dynamodb.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodb.model.AttributeValue;
import com.amazonaws.services.dynamodb.model.Key;

public abstract class DynamoDBRecordReader<T extends DynamoDBKeyWritable>
		extends RecordReader<LongWritable, T> {
	
	private Class<T> valueClass;

	private DynamoDBInputFormat.DynamoDBInputSplit split;

	private Configuration conf;

	private DynamoDBConfiguration dbConf;

	private AmazonDynamoDBClient client;

	private String[] fieldNames;

	private String tableName;
	
	protected LongWritable key = null;

	protected T value = null;
	
	protected long pos = 0;
	
	private Iterator<Map<String, AttributeValue>> iterator;
	
	private Key lastKey = null;
	
	public DynamoDBRecordReader(
			DynamoDBInputFormat.DynamoDBInputSplit split,
			Class<T> valueClass, 
			Configuration conf,
			AmazonDynamoDBClient client, 
			DynamoDBConfiguration dbConf,
			String[] fields, 
			String table) {
		this.valueClass = valueClass;
		this.split = split;
		this.conf = conf;
		this.client = client;
		this.dbConf = dbConf;
		this.fieldNames = fields;
		this.tableName = table;
	}
	
	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		if(key == null) {
			key = new LongWritable();
		}
		
		if(value == null) {
			value = createValue();
		}
		
		if(iterator == null) {
			executeQuery();
		}
		
		key.set(pos);
		
		if(iterator.hasNext()) {
			value.readFields(iterator.next());
		} else {
			iterator = null;
		}
		
		pos++;
		
		return (lastKey != null || iterator.hasNext());
	}
	
	protected abstract void executeQuery();

	@Override
	public void initialize(InputSplit inputSplit, TaskAttemptContext context)
			throws IOException, InterruptedException {
		// do nothing
	}
	
	@Override
	public void close() throws IOException {
		if (client != null) {
			client.shutdown();
		}
	}
	
	@Override
	public LongWritable getCurrentKey() throws IOException, InterruptedException {
		return key;
	}
	
	public void setCurrentKey(LongWritable key) {
		this.key = key;
	}

	@Override
	public T getCurrentValue() throws IOException, InterruptedException {
		return value;
	}
	
	public void setCurrentValue(T value) {
		this.value = value;
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
		return pos / Long.MAX_VALUE;
	}

	public T createValue() {
		return ReflectionUtils.newInstance(valueClass, conf);
	}

	protected Class<T> getValueClass() {
		return valueClass;
	}

	protected void setValueClass(Class<T> valueClass) {
		this.valueClass = valueClass;
	}

	protected DynamoDBInputFormat.DynamoDBInputSplit getSplit() {
		return split;
	}

	protected void setSplit(DynamoDBInputFormat.DynamoDBInputSplit split) {
		this.split = split;
	}

	protected Configuration getConf() {
		return conf;
	}

	protected void setConf(Configuration conf) {
		this.conf = conf;
	}

	protected DynamoDBConfiguration getDbConf() {
		return dbConf;
	}

	protected void setDbConf(DynamoDBConfiguration dbConf) {
		this.dbConf = dbConf;
	}

	protected AmazonDynamoDBClient getClient() {
		return client;
	}

	protected void setClient(AmazonDynamoDBClient client) {
		this.client = client;
	}

	protected String[] getFieldNames() {
		return fieldNames;
	}

	protected void setFieldNames(String[] fieldNames) {
		this.fieldNames = fieldNames;
	}

	protected String getTableName() {
		return tableName;
	}

	protected void setTableName(String tableName) {
		this.tableName = tableName;
	}

	protected Iterator<Map<String, AttributeValue>> getIterator() {
		return iterator;
	}
	
	protected Iterator<Map<String, AttributeValue>> iterator() {
		return iterator;
	}

	protected void setIterator(Iterator<Map<String, AttributeValue>> iterator) {
		this.iterator = iterator;
	}

	protected Key getLastKey() {
		return lastKey;
	}

	protected void setLastKey(Key lastKey) {
		this.lastKey = lastKey;
	}
	
}
