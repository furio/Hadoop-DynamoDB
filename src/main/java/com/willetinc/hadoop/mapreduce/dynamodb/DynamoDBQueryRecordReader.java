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
import com.amazonaws.services.dynamodb.model.Condition;
import com.amazonaws.services.dynamodb.model.QueryRequest;
import com.amazonaws.services.dynamodb.model.QueryResult;
import com.willetinc.hadoop.mapreduce.dynamodb.DynamoDBQueryInputFormat.DynamoDBQueryInputSplit;
import com.willetinc.hadoop.mapreduce.dynamodb.io.DynamoDBKeyWritable;

public class DynamoDBQueryRecordReader<T extends DynamoDBKeyWritable> extends
		DynamoDBRecordReader<T> {

	private static final Log LOG = LogFactory
			.getLog(DynamoDBQueryRecordReader.class);
	
	private QueryRequest queryRequest;

	public DynamoDBQueryRecordReader(DynamoDBQueryInputSplit inputSplit,
			Class<T> valueClass, Configuration conf,
			AmazonDynamoDBClient client, DynamoDBConfiguration dbConf,
			String table) {
		super(inputSplit, valueClass, conf, client, dbConf, table);
		
		queryRequest = new QueryRequest()
			.withTableName(getTableName())
			.withHashKeyValue(inputSplit.getHashKeyValue());
		
		// configure range key if it exists
		if(inputSplit.hasRangeKey()) {
			Condition condition = new Condition()
				.withComparisonOperator(inputSplit.getRangeKeyOperator())
				.withAttributeValueList(inputSplit.getRangeKeyValues());
			queryRequest.setRangeKeyCondition(condition);
		}
		
	}

	@Override
	protected void executeQuery() {
		if (LOG.isDebugEnabled()) {
			LOG.debug(String.format(
					"Querying table: %s from ExclusiveStartKey: %s",
					getTableName(), getLastKey()));
		}
		
		if (getLastKey() != null) {
			queryRequest.setExclusiveStartKey(getLastKey());
		}

		QueryResult result = getClient().query(queryRequest);
		setLastKey(result.getLastEvaluatedKey());
		setIterator(result.getItems().iterator());
	}
}
