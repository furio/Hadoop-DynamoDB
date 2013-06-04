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

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputSplit;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ComparisonOperator;

/**
 * Implements Splitter over DynamoDB Number datatype values.
 */
public class BigDecimalSplitter extends AbstractSplitter {

	private static final Log LOG = LogFactory.getLog(BigDecimalSplitter.class);

	private static final BigDecimal MIN_POSITIVE_VALUE = new BigDecimal(
			"0.0000000000000000000000000000000000001");

	@Override
	void generateRangeKeySplits(
			Configuration conf,
			List<InputSplit> splits,
			Types hashKeyType,
			AttributeValue hashKeyValue,
			Types rangeKeyType,
			AttributeValue minRangeKeyValue,
			AttributeValue maxRangeKeyValue,
			int numRangeSplits) {

		BigDecimal numSplits = BigDecimal.valueOf(numRangeSplits);
		BigDecimal minVal = new BigDecimal(minRangeKeyValue.getN());
		BigDecimal maxVal = new BigDecimal(maxRangeKeyValue.getN());

		// Get all the split points together.
		List<BigDecimal> splitPoints = split(numSplits, minVal, maxVal);

		// Turn the split points into a set of intervals.
		BigDecimal start = splitPoints.get(0);
		for (int i = 1; i < splitPoints.size(); i++) {
			BigDecimal end = splitPoints.get(i);

			List<AttributeValue> rangeKeyValues =
					new ArrayList<AttributeValue>();
			rangeKeyValues.add(new AttributeValue().withN(start.toString()));
			rangeKeyValues.add(new AttributeValue().withN(end.toString()));

			splits.add(new DynamoDBQueryInputFormat.DynamoDBQueryInputSplit(
					hashKeyType,
					hashKeyValue,
					rangeKeyType,
					rangeKeyValues,
					ComparisonOperator.BETWEEN));

			// set start to end of last interval plus minimum positive value
			// in the case of DynamoDB Numbers it is 1.0^-38:
			// This is necessary to ensure we don't miss any values between
			// intervals.
			start = end.add(MIN_POSITIVE_VALUE);
		}
	}

	private static final BigDecimal MIN_INCREMENT = new BigDecimal(
			10000 * Double.MIN_VALUE);

	/**
	 * Divide numerator by denominator. If impossible in exact mode, use
	 * rounding.
	 */
	protected static BigDecimal tryDivide(
			BigDecimal numerator,
			BigDecimal denominator) {
		try {
			return numerator.divide(denominator);
		} catch (ArithmeticException ae) {
			return numerator.divide(denominator, BigDecimal.ROUND_HALF_UP);
		}
	}

	/**
	 * <p>
	 * Returns a list of BigDecimals one element longer than the list of input
	 * splits. This represents the boundaries between input splits. All splits
	 * are open on the top end, except the last one.
	 * </p>
	 * 
	 * <p>
	 * So the list [0, 5, 8, 12, 18] would represent splits capturing the
	 * intervals:
	 * </p>
	 * 
	 * <p>
	 * The smallest positive value supported by DynamoDB 'e' is used to separate
	 * intervals
	 * </p>
	 * 
	 * <p>
	 * e = 0.0000000000000000000000000000000000001
	 * </p>
	 * 
	 * <p>
	 * [0, 5] [5+e, 8] [8+e, 12] [12+e, 18]
	 * </p>
	 */
	List<BigDecimal> split(
			BigDecimal numSplits,
			BigDecimal minVal,
			BigDecimal maxVal) {

		List<BigDecimal> splits = new ArrayList<BigDecimal>();

		// Use numSplits as a hint. May need an extra task if the size doesn't
		// divide cleanly.

		BigDecimal splitSize = tryDivide(maxVal.subtract(minVal), (numSplits));
		if (splitSize.compareTo(MIN_INCREMENT) < 0) {
			splitSize = MIN_INCREMENT;
			LOG.warn("Set BigDecimal splitSize to MIN_INCREMENT");
		}

		BigDecimal curVal = minVal;

		while (curVal.compareTo(maxVal) <= 0) {
			splits.add(curVal);
			curVal = curVal.add(splitSize);
		}

		if (splits.get(splits.size() - 1).compareTo(maxVal) != 0
				|| splits.size() == 1) {
			// We didn't end on the maxVal. Add that to the end of the list.
			splits.add(maxVal);
		}

		return splits;
	}

}
