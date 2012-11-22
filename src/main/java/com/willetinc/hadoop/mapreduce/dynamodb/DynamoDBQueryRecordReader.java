package com.willetinc.hadoop.mapreduce.dynamodb;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

import com.amazonaws.services.dynamodb.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodb.model.Condition;
import com.amazonaws.services.dynamodb.model.QueryRequest;
import com.amazonaws.services.dynamodb.model.QueryResult;
import com.willetinc.hadoop.mapreduce.dynamodb.DynamoDBQueryInputFormat.DynamoDBQueryInputSplit;

public class DynamoDBQueryRecordReader<T extends DynamoDBKeyWritable> extends
		DynamoDBRecordReader<T> {

	private static final Log LOG = LogFactory
			.getLog(DynamoDBQueryRecordReader.class);
	
	private QueryRequest queryRequest;

	public DynamoDBQueryRecordReader(DynamoDBQueryInputSplit inputSplit,
			Class<T> valueClass, Configuration conf,
			AmazonDynamoDBClient client, DynamoDBConfiguration dbConf,
			String[] fields, String table) {
		super(inputSplit, valueClass, conf, client, dbConf, fields, table);
		
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
