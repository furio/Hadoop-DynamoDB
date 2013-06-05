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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputSplit;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ComparisonOperator;

/**
 * This method needs to determine the splits between two user-provided strings.
 * In the case where the user's strings are 'A' and 'Z', this is not hard; we 
 * could create two splits from ['A', 'M') and ['M', 'Z'], 26 splits for strings
 * beginning with each letter, etc.
 *
 * If a user has provided us with the strings "Ham" and "Haze", however, we need
 * to create splits that differ in the third letter.
 *
 * The algorithm used is as follows:
 * Since there are 2**8 UTF8 unicode characters, we interpret characters as digits in
 * base 265. Given a string 's' containing characters s_0, s_1 .. s_n, we interpret
 * the string as the number: 0.s_0 s_1 s_2.. s_n in base 256. Having mapped the
 * low and high strings into floating-point values, we then use the BigDecimalSplitter
 * to establish the even split points, then map the resulting floating point values
 * back into strings.
 */
public class TextSplitter extends BigDecimalSplitter {
	
	private final static String FRIST_PRINTABLE_CHAR = new String(new byte[] {0x20});

	@Override
	protected void generateRangeKeySplits(
			Configuration conf,
			List<InputSplit> splits,
			Types hashKeyType,
			AttributeValue hashKeyValue,
			String hashKeyName,
			Types rangeKeyType,
			AttributeValue minRangeKeyValue,
			AttributeValue maxRangeKeyValue,
			String rangeKeyName,
			int numRangeSplits) {
		
		String minString = minRangeKeyValue.getS();
		String maxString = maxRangeKeyValue.getS();
		
		// If there is a common prefix between minString and maxString,
		// establish it
		// and pull it out of minString and maxString.
		int maxPrefixLen = Math.min(minString.length(), maxString.length());
		int sharedLen;
		for (sharedLen = 0; sharedLen < maxPrefixLen; sharedLen++) {
			char c1 = minString.charAt(sharedLen);
			char c2 = maxString.charAt(sharedLen);
			if (c1 != c2) {
				break;
			}
		}

		// The common prefix has length 'sharedLen'. Extract it from both.
		String commonPrefix = minString.substring(0, sharedLen);
		minString = minString.substring(sharedLen);
		maxString = maxString.substring(sharedLen);

		List<BigDecimal> splitStrings =
				split(numRangeSplits, minString, maxString);
		
		// Convert the list of split point strings into an actual set of
		// InputSplits.
		String start = commonPrefix + bigDecimalToString(splitStrings.get(0), MAX_CHARS);
		for (int i = 1; i < splitStrings.size(); i++) {
			String end = commonPrefix + bigDecimalToString(splitStrings.get(i), MAX_CHARS);

			if(compareStrings(start, end) >= 0) continue;
			
			List<AttributeValue> rangeKeyValues =
					new ArrayList<AttributeValue>();
			rangeKeyValues.add(new AttributeValue().withS(start));
			rangeKeyValues.add(new AttributeValue().withS(end));

			splits.add(new DynamoDBQueryInputFormat.DynamoDBQueryInputSplit(
					hashKeyType,
					hashKeyValue,
					hashKeyName,
					rangeKeyType,
					rangeKeyValues,
					rangeKeyName,
					ComparisonOperator.BETWEEN));

			start = end + FRIST_PRINTABLE_CHAR;
		}
	}
	
	public static int compareStrings(String a, String b) {
		for(int i = 0; i < a.length() && i < b.length(); i++) {
			if(a.charAt(i) < b.charAt(i)) {
				return -1;
			} else if(a.charAt(i) > b.charAt(i)) {
				return 1;
			}
		}
		
		return (a.length() < b.length()) ? -1 : (a.length() > b.length()) ? 1 : 0;
	}

	List<BigDecimal> split(int numSplits, String minString, String maxString) {

		BigDecimal minVal = stringToBigDecimal(minString, MAX_CHARS);
		BigDecimal maxVal = stringToBigDecimal(maxString, MAX_CHARS);

		List<BigDecimal> splitPoints =
				split(new BigDecimal(numSplits), minVal, maxVal);
		List<BigDecimal> splitStrings = new ArrayList<BigDecimal>();

		// Convert the BigDecimal splitPoints into their string representations.
		for (BigDecimal bd : splitPoints) {
			splitStrings.add(bd);
		}

		// Make sure that our user-specified boundaries are the first and last
		// entries
		// in the array.
		if (splitStrings.size() == 0
				|| (0 != splitStrings.get(0).compareTo(minVal))) {
			splitStrings.add(0, minVal);
		}
		if (splitStrings.size() == 1
				|| (0 != splitStrings.get(splitStrings.size() - 1).compareTo(
						maxVal))) {
			splitStrings.add(maxVal);
		}

		return splitStrings;
	}

	private final static BigDecimal ONE_PLACE = new BigDecimal(256);

	// Maximum number of characters to convert. This is to prevent rounding
	// errors
	// or repeating fractions near the very bottom from getting out of control.
	// Note
	// that this still gives us a huge number of possible splits.
	private final static int MAX_CHARS = 8;
	
	/**
	 * Return a BigDecimal representation of string 'str' suitable for use in a
	 * numerically-sorting order.
	 */
	BigDecimal stringToBigDecimal(String str, int maxChars) {
		BigDecimal result = BigDecimal.ZERO;
		BigDecimal curPlace = ONE_PLACE; // start with 1/256 to compute the
											// first digit.

		int len = Math.min(str.length(), maxChars);

		for (int i = 0; i < len; i++) {
			int codePoint = str.codePointAt(i);
			result = result.add(tryDivide(new BigDecimal(codePoint), curPlace));
			// advance to the next less significant place. e.g., 1/(256^2) for
			// the second char.
			curPlace = curPlace.multiply(ONE_PLACE);
		}

		return result;
	}

	/**
	 * Return the string encoded in a BigDecimal. Repeatedly multiply the input
	 * value by 256; the integer portion after such a multiplication
	 * represents a single character in base 256. Convert that back into a
	 * char and create a string out of these until we have no data left.
	 */
	String bigDecimalToString(BigDecimal bd, int maxChars) {
		BigDecimal cur = bd.stripTrailingZeros();
		StringBuilder sb = new StringBuilder();

		for (int numConverted = 0; numConverted < maxChars; numConverted++) {
			cur = cur.multiply(ONE_PLACE);
			int curCodePoint = cur.intValue();
			if (0 == curCodePoint) {
				break;
			}

			cur = cur.subtract(new BigDecimal(curCodePoint));
			sb.append(Character.toChars(curCodePoint));
		}

		return sb.toString();
	}

}
