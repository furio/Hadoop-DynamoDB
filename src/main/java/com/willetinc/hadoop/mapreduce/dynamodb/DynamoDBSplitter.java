package com.willetinc.hadoop.mapreduce.dynamodb;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputSplit;

/**
 * Splitter will generate InputSpits for use with DynamoDBQueryInputFormat.
 * DynamoDBQueryInputFormat needs to partition HashKey values and optionally
 * interpolate between two RangeKey values that represent the lowest and 
 * highest valued records to import. Depending on the data-type of the column,
 * this requires different behavior. DBSplitter implementations should perform 
 * this for a data type or family of data types.
 */
public interface DynamoDBSplitter {

	/**
	 * <p>Generates input splits across values in a DynamoDB table.</p>
	 * 
	 * <p>There are two ways we can split the input table:</p>
	 * <ol>
	 * 		<li>Across HashKey values (hashKey = value)</li>
	 * 		<li>Across HashKey values and within rangeKeyValues <br />
	 *    	(hashKey = value && (rangeKey BETWEEN value1 and value2))</li>
	 * </ol>
	 * 
	 * @param conf Hadoop configuration
	 * @return Generated InputSplits
	 * @throws IOException Error generating input splits.
	 */
	List<InputSplit> split(Configuration conf) throws IOException;
	
}
