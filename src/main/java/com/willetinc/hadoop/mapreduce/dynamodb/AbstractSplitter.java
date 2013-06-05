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
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputSplit;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ComparisonOperator;

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
		boolean interpolate = DynamoDBQueryInputFormat.getInterpolateAcrossRangeKeyValues(conf);

		Types hashKeyType = DynamoDBQueryInputFormat.getHashKeyType(conf);
		AttributeValue hashKeyValue = DynamoDBQueryInputFormat.getHashKeyValue(conf);
		String hashKeyName = DynamoDBQueryInputFormat.getHashKeyName(conf);

		Types rangeKeyType = DynamoDBQueryInputFormat.getRangeKeyType(conf);
		Collection<AttributeValue> rangeKeyValues = DynamoDBQueryInputFormat.getRangeKeyValues(conf);
		String rangeKeyName = DynamoDBQueryInputFormat.getRangeKeyName(conf);
		ComparisonOperator rangeKeyoperator = DynamoDBQueryInputFormat.getRangeKeyComparisonOperator(conf);
		AttributeValue minRangeKeyValue = DynamoDBQueryInputFormat.getRangeKeyInterpolateMinValue(conf);
		AttributeValue maxRangeKeyValue = DynamoDBQueryInputFormat.getRangeKeyInterpolateMaxValue(conf);

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
		if (!interpolate
				|| numRangeSplits <= 1
				|| minRangeKeyValue == null
				|| maxRangeKeyValue == null) {
			LOG.info("Generating 1 split for each HashKey");

			DynamoDBQueryInputSplit split = new DynamoDBQueryInputSplit(
					hashKeyType,
					hashKeyValue,
					hashKeyName,
					rangeKeyType,
					rangeKeyValues,
					rangeKeyName,
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
						hashKeyName,
						rangeKeyType,
						minRangeKeyValue,
						maxRangeKeyValue,
						rangeKeyName,
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
			String hashKeyName,
			Types rangeKeyType,
			AttributeValue minRangeKeyValue,
			AttributeValue maxRangeKeyValue,
			String rangeKeyName,
			int numRangeSplits);

}
