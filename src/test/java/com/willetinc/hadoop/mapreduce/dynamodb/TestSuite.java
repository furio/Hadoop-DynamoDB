package com.willetinc.hadoop.mapreduce.dynamodb;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;

@RunWith(Suite.class)
@Suite.SuiteClasses({
		AbstractSplitterTest.class,
		AttributeValueIOUtilsTest.class,
		BigDecimalSplitterTest.class,
		BinarySplitterTest.class,
		DynamoDBQueryInputFormatTest.class,
		DynamoDBQueryRecordReaderTest.class,
		DynamoDBScanRecordReaderTest.class,
		TextSplitterTest.class })
public class TestSuite {
	// nothing
}
