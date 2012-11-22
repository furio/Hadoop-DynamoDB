package com.willetinc.hadoop.mapreduce.dynamodb;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputSplit;

import com.amazonaws.services.dynamodb.model.AttributeValue;
import com.amazonaws.services.dynamodb.model.ComparisonOperator;

/**
 * <p>
 * Implements Splitter over DynamoDB Number datatype values.
 * </p>
 * <p>
 * 
 * </p>
 */
public abstract class AbstractSplitter implements DynamoDBSplitter {

	private static final Log LOG = LogFactory.getLog(AbstractSplitter.class);

	@Override
	public List<InputSplit> split(Configuration conf) throws IOException {

		// load configuration
		boolean interpolate =
				DynamoDBQueryInputFormat
						.getInterpolateAcrossRangeKeyValues(conf);

		Types hashKeyType = DynamoDBQueryInputFormat.getHashKeyType(conf);
		AttributeValue hashKeyValue =
				DynamoDBQueryInputFormat.getHashKeyValue(conf);

		Types rangeKeyType = DynamoDBQueryInputFormat.getRangeKeyType(conf);
		Collection<AttributeValue> rangeKeyValues =
				DynamoDBQueryInputFormat.getRangeKeyValues(conf);
		ComparisonOperator rangeKeyoperator =
				DynamoDBQueryInputFormat.getRangeKeyComparisonOperator(conf);
		AttributeValue minRangeKeyValue =
				DynamoDBQueryInputFormat.getRangeKeyInterpolateMinValue(conf);
		AttributeValue maxRangeKeyValue =
				DynamoDBQueryInputFormat.getRangeKeyInterpolateMaxValue(conf);

		// ensure DynamoDBQueryInputFormat was configured correctly
		if (interpolate) {
			rangeKeyValues = new ArrayList<AttributeValue>();
		} else {
			minRangeKeyValue = null;
			maxRangeKeyValue = null;
		}

		// compute number of input splits
		int numSplits = conf.getInt("mapred.map.tasks", 1);
		int numHashKeys = 1;
		int numRangeSplits = numSplits / numHashKeys;
		numRangeSplits = (!interpolate) ? 1 : numRangeSplits;
		numRangeSplits = (numRangeSplits <= 0) ? 1 : numRangeSplits;

		// generate input spits
		List<InputSplit> splits = new ArrayList<InputSplit>();

		// handle cases where interpolation is turned off or unnecessary
		if (!interpolate || numRangeSplits <= 1 || minRangeKeyValue == null
				|| maxRangeKeyValue == null) {
			LOG.info("Generating 1 split for each HashKey");

			DynamoDBQueryInputFormat.DynamoDBQueryInputSplit split =
					new DynamoDBQueryInputFormat.DynamoDBQueryInputSplit(
							hashKeyType,
							hashKeyValue,
							rangeKeyType,
							rangeKeyValues,
							rangeKeyoperator);

			splits.add(split);
		} else {
			// interpolate between RangeKey values
			LOG.info(String.format(
					"Generating %d RangeKey splits for each HashKey",
					numRangeSplits));

			if (null == hashKeyValue) {
				LOG.error("Cannot create a range when the HashKey is NULL. Ignoring range key interpolation.");
			} else {
				generateRangeKeySplits(
						conf,
						splits,
						hashKeyType,
						hashKeyValue,
						rangeKeyType,
						minRangeKeyValue,
						maxRangeKeyValue,
						numRangeSplits);
			}
		}

		return splits;
	}

	abstract void generateRangeKeySplits(
			Configuration conf,
			List<InputSplit> splits,
			Types hashKeyType,
			AttributeValue hashKeyValue,
			Types rangeKeyType,
			AttributeValue minRangeKeyValue,
			AttributeValue maxRangeKeyValue,
			int numRangeSplits);

}
