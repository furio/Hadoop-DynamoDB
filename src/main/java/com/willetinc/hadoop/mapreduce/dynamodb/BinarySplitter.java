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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputSplit;

import com.amazonaws.services.dynamodb.model.AttributeValue;
import com.amazonaws.services.dynamodb.model.ComparisonOperator;

/**
 * This method needs to determine the splits between two user-provided byte
 * arrays. In the case where the user's bytes are 0x0 and 0xF, this is not hard;
 * we could create two splits from [0x0, 0x7] and [0x8, 0xF], 16 splits for
 * bytes.
 * 
 * If a user has provided us with the byte arrays "[0xD, 0xA, 0xD]" and [0xD,
 * 0xA, 0xB], however, we need to create splits that differ in the third byte.
 * 
 * The algorithm used is as follows: Since there are 16 values per bit, we
 * interpret byts as digits in base 16. Given a byte array b containing bytes
 * b_0, b_1 .. b_n, we interpret the string as the number: 0.b_0 b_1 b_2.. b_n
 * in base 16. Having mapped the low and high strings into floating-point
 * values, we then use the BigDecimalSplitter to establish the even split
 * points, then map the resulting floating point values back into byte arrays.
 */
public class BinarySplitter extends BigDecimalSplitter {

	private static final Log LOG = LogFactory.getLog(BinarySplitter.class);

	private final static int MAX_BYTES = 16;

	private final static BigDecimal ONE_PLACE = new BigDecimal(16);

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

		byte[] minBytes = minRangeKeyValue.getB().array();
		byte[] maxBytes = maxRangeKeyValue.getB().array();

		// If there is a common prefix between minString and maxString,
		// establish it
		// and pull it out of minString and maxString.
		int maxPrefixLen = Math.min(minBytes.length, maxBytes.length);
		int sharedLen;
		for (sharedLen = 0; sharedLen < maxPrefixLen; sharedLen++) {
			byte b1 = minBytes[sharedLen];
			byte b2 = maxBytes[sharedLen];
			if (b1 != b2) {
				break;
			}
		}

		// The common prefix has length 'sharedLen'. Extract it from both.
		byte[] commonPrefix = Arrays.copyOfRange(minBytes, 0, sharedLen);
		minBytes = Arrays.copyOfRange(minBytes, sharedLen, minBytes.length);
		maxBytes = Arrays.copyOfRange(maxBytes, sharedLen, maxBytes.length);

		List<BigDecimal> splitValues =
				split(numRangeSplits, minBytes, maxBytes);

		// Convert the list of split point strings into an actual set of
		// InputSplits.
		byte[] start =
				ArrayUtils.addAll(
						commonPrefix,
						bigDecimalToByteArray(splitValues.get(0), MAX_BYTES));
		for (int i = 1; i < splitValues.size(); i++) {
			byte[] end =
					ArrayUtils
							.addAll(
									commonPrefix,
									bigDecimalToByteArray(
											splitValues.get(i),
											MAX_BYTES));

			//if (compareBytes(start, end) >= 0)
			//	continue;

			List<AttributeValue> rangeKeyValues =
					new ArrayList<AttributeValue>();
			rangeKeyValues.add(new AttributeValue().withB(ByteBuffer
					.wrap(start)));
			rangeKeyValues
					.add(new AttributeValue().withB(ByteBuffer.wrap(end)));

			splits.add(new DynamoDBQueryInputFormat.DynamoDBQueryInputSplit(
					hashKeyType,
					hashKeyValue,
					rangeKeyType,
					rangeKeyValues,
					ComparisonOperator.BETWEEN));

			start = ArrayUtils.addAll(end, new byte[] { 0x0 });
		}
	}

	public static int compareBytes(byte[] a, byte[] b) {
		for (int i = 0; i < a.length && i < b.length; i++) {
			if (a[i] < b[i]) {
				return -1;
			} else if (a[i] > b[i]) {
				return 1;
			}
		}

		return (a.length < b.length) ? -1 : (a.length > b.length) ? 1 : 0;
	}

	List<BigDecimal> split(int numSplits, byte[] minBytes, byte[] maxBytes) {

		BigDecimal minVal = byteArrayToBigDecimal(minBytes, MAX_BYTES);
		BigDecimal maxVal = byteArrayToBigDecimal(maxBytes, MAX_BYTES);

		List<BigDecimal> splitPoints =
				split(new BigDecimal(numSplits), minVal, maxVal);
		List<BigDecimal> splitValues = new ArrayList<BigDecimal>();

		// Convert the BigDecimal splitPoints into their string representations.
		for (BigDecimal bd : splitPoints) {
			splitValues.add(bd);
		}

		// Make sure that our user-specified boundaries are the first and last
		// entries in the array.
		if (splitValues.size() == 0
				|| (0 != splitValues.get(0).compareTo(minVal))) {
			splitValues.add(0, minVal);
		}
		if (splitValues.size() == 1
				|| (0 != splitValues.get(splitValues.size() - 1).compareTo(
						maxVal))) {
			splitValues.add(maxVal);
		}

		return splitValues;
	}

	/**
	 * Return a BigDecimal representation of byte[] array suitable for use in a
	 * numerically-sorting order.
	 */
	static BigDecimal byteArrayToBigDecimal(byte[] array, int maxBytes) {
		BigDecimal result = BigDecimal.ZERO;
		BigDecimal curPlace = ONE_PLACE; // start with 1/16 to compute the
											// first digit.

		int len = Math.min(array.length, maxBytes);

		for (int i = 0; i < len; i++) {
			byte codePoint = array[i];
			result = result.add(tryDivide(new BigDecimal(codePoint), curPlace));
			// advance to the next less significant place. e.g., 1/(16^2) for
			// the second char.
			curPlace = curPlace.multiply(ONE_PLACE);
		}

		return result;
	}

	/**
	 * Return the string encoded in a BigDecimal. Repeatedly multiply the input
	 * value by 16; the integer portion after such a multiplication represents a
	 * single character in base 16. Convert that back into a char and create a
	 * string out of these until we have no data left.
	 * 
	 * @throws IOException
	 */
	static byte[] bigDecimalToByteArray(BigDecimal bd, int maxBytes) {
		BigDecimal cur = bd.stripTrailingZeros();
		ByteArrayOutputStream sb = new ByteArrayOutputStream();

		try {
			byte[] curCodePoint = new byte[1];
			for (int numConverted = 0; numConverted < maxBytes; numConverted++) {
				cur = cur.multiply(ONE_PLACE);
				curCodePoint[0] = cur.byteValue();
				if (0x0 == curCodePoint[0]) {
					break;
				}

				cur =
						cur.subtract(new BigDecimal(
								new BigInteger(curCodePoint)));
				sb.write(curCodePoint);
			}
		} catch (IOException e) {
			LOG.error("Error writing byte array", e);
		}

		return sb.toByteArray();
	}

}
