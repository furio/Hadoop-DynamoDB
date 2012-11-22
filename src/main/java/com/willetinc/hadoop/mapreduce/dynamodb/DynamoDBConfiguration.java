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

import org.apache.hadoop.conf.Configuration;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.dynamodb.AmazonDynamoDBClient;
import com.willetinc.hadoop.mapreduce.dynamodb.DynamoDBInputFormat.NullDynamoDBWritable;
import com.willetinc.hadoop.mapreduce.dynamodb.io.DynamoDBKeyWritable;

public class DynamoDBConfiguration {

	public final static String ACCESS_KEY_PROPERTY = "mapreduce.dynamodb.access.key";

	public final static String SECRET_KEY_PROPERTY = "mapreduce.dynamodb.secret.key";

	public final static String INPUT_TABLE_NAME_PROPERTY = "mapreduce.dynamodb.input.talble.name";

	public static final String INPUT_CLASS_PROPERTY = "mapreduce.dynamodb.input.class";

	public static final String OUTPUT_TABLE_NAME_PROPERTY = "mapreduce.dynamodb.output.table.name";

	public static final String HASH_KEY_TYPE_PROPERTY = "mapreduce.dynamodb.query.hashkey.type";
	
	public static final String HASH_KEY_VALUE_PROPERTY = "mapreduce.dynamodb.query.hashkey.value";
	
	public static final String RANGE_KEY_TYPE_PROPERTY = "mapreduce.dynamodb.query.rangekey.type";
	
	public static final String RANGE_KEY_VALUES_PROPERTY = "mapreduce.dynamodb.query.rangekey.values";
	
	public static final String RANGE_KEY_OPERATOR_PROPERTY = "mapreduce.dynamodb.query.rangekey.operator";

	public static final String RANGE_KEY_INTERPOLATE_PROPERTY = "mapreduce.dynamodb.query.rangekey.interpolate";
	
	public static final String RANGE_KEY_INTERPOLATE_MIN_VALUE_PROPERTY = "mapreduce.dynamodb.query.rangekey.interpolate.minvalue";
	
	public static final String RANGE_KEY_INTERPOLATE_MAX_VALUE_PROPERTY = "mapreduce.dynamodb.query.rangekey.interpolate.maxvalue";
	
	public static void configureDB(
			Configuration conf, 
			String tableName, 
			String accessKey, 
			String secretKey ) {
		conf.set(INPUT_TABLE_NAME_PROPERTY, tableName);
		conf.set(ACCESS_KEY_PROPERTY, accessKey);
		conf.set(SECRET_KEY_PROPERTY, secretKey);
	}

	private Configuration conf;

	public DynamoDBConfiguration(Configuration conf) {
		this.conf = conf;
	}

	public Configuration getConf() {
		return conf;
	}

	public AmazonDynamoDBClient getAmazonDynamoDBClient() {
		String accessKey = conf.get(ACCESS_KEY_PROPERTY);
		String secretKey = conf.get(SECRET_KEY_PROPERTY);
		AWSCredentials credentials = new BasicAWSCredentials(accessKey,
				secretKey);
		return new AmazonDynamoDBClient(credentials);
	}

	public String getInputTableName() {
		return conf.get(INPUT_TABLE_NAME_PROPERTY);
	}

	public void setInputTableName(String tableName) {
		conf.set(INPUT_TABLE_NAME_PROPERTY, tableName);
	}

	public Class<?> getInputClass() {
		return conf.getClass(INPUT_CLASS_PROPERTY, NullDynamoDBWritable.class);
	}

	public void setInputClass(Class<? extends DynamoDBKeyWritable> inputClass) {
		conf.setClass(INPUT_CLASS_PROPERTY, inputClass,
				NullDynamoDBWritable.class);
	}

	public String getOutputTableName() {
		return conf.get(OUTPUT_TABLE_NAME_PROPERTY);
	}

	public void setOutputTableName(String tableName) {
		conf.set(OUTPUT_TABLE_NAME_PROPERTY, tableName);
	}
}
