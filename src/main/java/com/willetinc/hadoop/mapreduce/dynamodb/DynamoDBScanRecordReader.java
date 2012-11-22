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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

import com.amazonaws.services.dynamodb.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodb.model.ScanRequest;
import com.amazonaws.services.dynamodb.model.ScanResult;
import com.willetinc.hadoop.mapreduce.dynamodb.DynamoDBInputFormat.DynamoDBInputSplit;
import com.willetinc.hadoop.mapreduce.dynamodb.io.DynamoDBKeyWritable;

public class DynamoDBScanRecordReader<T extends DynamoDBKeyWritable> extends DynamoDBRecordReader<T> {

	private static final Log LOG = LogFactory.getLog(DynamoDBScanRecordReader.class);
	
	public DynamoDBScanRecordReader(
			DynamoDBInputSplit split,
			Class<T> valueClass, 
			Configuration conf,
			AmazonDynamoDBClient client, 
			DynamoDBConfiguration dbConf,
			String table) {
		super(split, valueClass, conf, client, dbConf, table);
	}
	
	@Override
	protected void executeQuery() {
		if (LOG.isDebugEnabled())
			LOG.debug(String.format("Scaning of table: %s from ExclusiveStartKey: %s", getTableName(), getLastKey()));
		
		ScanRequest scanRequest = new ScanRequest().withTableName(getTableName());
		if(getLastKey() != null) {
			scanRequest.setExclusiveStartKey(getLastKey());
		}
		ScanResult resultSet = getClient().scan(scanRequest);
		setLastKey(resultSet.getLastEvaluatedKey());
		setIterator(resultSet.getItems().iterator());
	}
}
