package com.willetinc.hadoop.mapreduce.dynamodb;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

import com.amazonaws.services.dynamodb.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodb.model.ScanRequest;
import com.amazonaws.services.dynamodb.model.ScanResult;
import com.willetinc.hadoop.mapreduce.dynamodb.DynamoDBInputFormat.DynamoDBInputSplit;

public class DynamoDBScanRecordReader<T extends DynamoDBKeyWritable> extends DynamoDBRecordReader<T> {

	private static final Log LOG = LogFactory.getLog(DynamoDBScanRecordReader.class);
	
	public DynamoDBScanRecordReader(
			DynamoDBInputSplit split,
			Class<T> valueClass, 
			Configuration conf,
			AmazonDynamoDBClient client, 
			DynamoDBConfiguration dbConf,
			String[] fields, 
			String table) {
		super(split, valueClass, conf, client, dbConf, fields, table);
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
