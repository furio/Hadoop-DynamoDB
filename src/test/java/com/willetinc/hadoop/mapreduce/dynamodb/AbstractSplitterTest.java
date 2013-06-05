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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.junit.Test;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ComparisonOperator;
import com.willetinc.hadoop.mapreduce.dynamodb.AbstractSplitter;
import com.willetinc.hadoop.mapreduce.dynamodb.DynamoDBQueryInputFormat;
import com.willetinc.hadoop.mapreduce.dynamodb.DynamoDBSplitter;
import com.willetinc.hadoop.mapreduce.dynamodb.Types;
import com.willetinc.hadoop.mapreduce.dynamodb.DynamoDBQueryInputFormat.DynamoDBQueryInputSplit;

public class AbstractSplitterTest {

	@Test
	public void testSplitWithHashKey() throws IOException {
		DynamoDBSplitter splitter = new AbstractSplitter() {

			@Override
			public void generateRangeKeySplits(
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
				fail("This method should not be called!");

			}
		};

		final String VALUE = "007";
		Types hashKeyType = Types.NUMBER;
		AttributeValue hashKeyValue = new AttributeValue().withN(VALUE);
		
		// configure job
		Job job = new Job();
		Configuration conf = job.getConfiguration();
		DynamoDBQueryInputFormat.setHashKeyValue(conf, hashKeyType, hashKeyValue);

		// generate splits
		List<InputSplit> splits = splitter.split(conf);
		
		// check results
		assertEquals(1, splits.size());
		DynamoDBQueryInputSplit split = (DynamoDBQueryInputSplit) splits.get(0);
		
		assertTrue(split.hasHashKey());
		assertEquals(hashKeyType, split.getHashKeyType());
		assertEquals(hashKeyValue, split.getHashKeyValue());
		
		assertFalse(split.hasRangeKey());
	}
	
	@Test
	public void testSplitWithHashKeyAndRangeKey() throws IOException {
		DynamoDBSplitter splitter = new AbstractSplitter() {

			@Override
			public void generateRangeKeySplits(
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
				fail("This method should not be called!");

			}
		};

		final String VALUE = "007";
		Types hashKeyType = Types.NUMBER;
		AttributeValue hashKeyValue = new AttributeValue().withN(VALUE);
		Types rangeKeyType = Types.NUMBER;
		List<AttributeValue> rangeKeyValues = new ArrayList<AttributeValue>();
		rangeKeyValues.add(new AttributeValue().withN("005"));
		ComparisonOperator rangeKeyOperator = ComparisonOperator.GT;
		
		// configure job
		Job job = new Job();
		Configuration conf = job.getConfiguration();
		DynamoDBQueryInputFormat.setHashKeyValue(conf, hashKeyType, hashKeyValue);
		DynamoDBQueryInputFormat.setRangeKeyCondition(conf, rangeKeyType, rangeKeyOperator, rangeKeyValues);

		// generate splits
		List<InputSplit> splits = splitter.split(conf);
		
		// verify results
		assertEquals(1, splits.size());
		DynamoDBQueryInputSplit split = (DynamoDBQueryInputSplit) splits.get(0);
		
		assertTrue(split.hasHashKey());
		assertEquals(hashKeyType, split.getHashKeyType());
		assertEquals(hashKeyValue, split.getHashKeyValue());
		
		assertTrue(split.hasRangeKey());
		assertEquals(rangeKeyType, split.getRangeKeyType());
		assertEquals(rangeKeyOperator, split.getRangeKeyOperator());
		assertEquals(1, split.getRangeKeyValues().size());
	}

	@Test
	public void testSplitWithHashKeyAndInterpolation() throws IOException {
		
		final int NUM_MAP_TASKS = 2; 
		final String VALUE = "007";
		final Types hashKeyType = Types.NUMBER;
		final AttributeValue hashKeyValue = new AttributeValue().withN(VALUE);
		final String hashKeyName = "Id";
		final Types rangeKeyType = Types.NUMBER;
		final AttributeValue minRangeKeyValue = new AttributeValue().withN(Long.toString(Long.MIN_VALUE));
		final AttributeValue maxRangeKeyValue = new AttributeValue().withN(Long.toString(Long.MIN_VALUE));
		final String rangeKeyName = "range";
		
		DynamoDBSplitter splitter = new AbstractSplitter() {

			@Override
			public void generateRangeKeySplits(
					Configuration conf,
					List<InputSplit> splits,
					Types inHashKeyType,
					AttributeValue inHashKeyValue,
					String inHashKeyName,
					Types inRangeKeyType,
					AttributeValue inMinRangeKeyValue,
					AttributeValue inMaxRangeKeyValue,
					String inRangeKeyName,
					int numRangeSplits) {
				
				// check values
				assertEquals(hashKeyType, inHashKeyType);
				assertEquals(hashKeyValue, inHashKeyValue);
				assertEquals(hashKeyName, inHashKeyName);
				assertEquals(rangeKeyType, inRangeKeyType);
				assertEquals(minRangeKeyValue, inMinRangeKeyValue);
				assertEquals(maxRangeKeyValue, inMaxRangeKeyValue);
				assertEquals(rangeKeyName, inRangeKeyName);
				assertEquals(NUM_MAP_TASKS, numRangeSplits);
				
				List<AttributeValue> rangeKeyValues = new ArrayList<AttributeValue>();
				rangeKeyValues.add(inMinRangeKeyValue);
				rangeKeyValues.add(inMaxRangeKeyValue);
				
				DynamoDBQueryInputFormat.DynamoDBQueryInputSplit split =
						new DynamoDBQueryInputFormat.DynamoDBQueryInputSplit(
								hashKeyType,
								hashKeyValue,
								hashKeyName,
								rangeKeyType,
								rangeKeyValues,
								rangeKeyName,
								ComparisonOperator.BETWEEN);
				
				splits.add(split);
			}
		};

		// configure job
		Job job = new Job();
		Configuration conf = job.getConfiguration();
		conf.setInt("mapred.map.tasks", NUM_MAP_TASKS);
		DynamoDBQueryInputFormat.setHashKeyValue(conf, hashKeyType, hashKeyValue);
		DynamoDBQueryInputFormat.setHashKeyName(conf, hashKeyName);
		DynamoDBQueryInputFormat.setRangeKeyType(conf, rangeKeyType);
		DynamoDBQueryInputFormat.setRangeKeyInterpolateRange(conf, rangeKeyType, minRangeKeyValue, maxRangeKeyValue);
		DynamoDBQueryInputFormat.setRangeKeyName(conf, rangeKeyName);

		// generate input splits
		List<InputSplit> splits = splitter.split(conf);
		assertEquals(1, splits.size());
		
		DynamoDBQueryInputSplit split = (DynamoDBQueryInputSplit) splits.get(0);
		
		// check results
		assertTrue(split.hasHashKey());
		assertEquals(hashKeyType, split.getHashKeyType());
		assertEquals(hashKeyValue, split.getHashKeyValue());
		
		assertTrue(split.hasRangeKey());
		assertEquals(rangeKeyType, split.getRangeKeyType());
		assertEquals(ComparisonOperator.BETWEEN, split.getRangeKeyOperator());
		assertEquals(2, split.getRangeKeyValues().size());
		
		Iterator<AttributeValue> itr = split.getRangeKeyValues().iterator();
		assertEquals(minRangeKeyValue, itr.next());
		assertEquals(maxRangeKeyValue, itr.next());
	}

}
