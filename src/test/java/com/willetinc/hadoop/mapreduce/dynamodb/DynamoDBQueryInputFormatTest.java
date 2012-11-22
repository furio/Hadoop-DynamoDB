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

import static org.easymock.EasyMock.*;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

import com.amazonaws.services.dynamodb.model.AttributeValue;
import com.amazonaws.services.dynamodb.model.ComparisonOperator;
import com.willetinc.hadoop.mapreduce.dynamodb.DynamoDBConfiguration;
import com.willetinc.hadoop.mapreduce.dynamodb.DynamoDBQueryInputFormat;
import com.willetinc.hadoop.mapreduce.dynamodb.Types;

public class DynamoDBQueryInputFormatTest {

	@Test
	public void testGetHashKeyType() {
		Configuration conf = createMock(Configuration.class);
		expect(
				conf.getInt(
						DynamoDBConfiguration.HASH_KEY_TYPE_PROPERTY,
						Types.STRING.ordinal())).andReturn(
				Types.NUMBER.ordinal());

		replay(conf);
		Types type = DynamoDBQueryInputFormat.getHashKeyType(conf);
		assertEquals(Types.NUMBER, type);
		verify(conf);
	}

	@Test
	public void testSetHashKeyType() {
		Configuration conf = createMock(Configuration.class);

		conf.setInt(
				DynamoDBConfiguration.HASH_KEY_TYPE_PROPERTY,
				Types.NUMBER.ordinal());

		replay(conf);
		DynamoDBQueryInputFormat.setHashKeyType(conf, Types.NUMBER);
		verify(conf);
	}

	@Test
	public void testSetHashKeyValue() {
		Configuration conf = createMock(Configuration.class);
		final String VALUE = "TEST";
		Types type = Types.STRING;
		AttributeValue attr = new AttributeValue().withS(VALUE);

		conf.setInt(
				DynamoDBConfiguration.HASH_KEY_TYPE_PROPERTY,
				type.ordinal());
		conf.set(DynamoDBConfiguration.HASH_KEY_VALUE_PROPERTY, VALUE);

		replay(conf);

		DynamoDBQueryInputFormat.setHashKeyValue(conf, type, attr);

		verify(conf);
	}

	@Test
	public void testGetHashKeyValue() {
		Configuration conf = createMock(Configuration.class);
		final String VALUE = "TEST";
		Types type = Types.STRING;

		expect(
				conf.getInt(
						DynamoDBConfiguration.HASH_KEY_TYPE_PROPERTY,
						Types.STRING.ordinal())).andReturn(type.ordinal());
		expect(conf.get(DynamoDBConfiguration.HASH_KEY_VALUE_PROPERTY))
				.andReturn(VALUE);
		expect(
				conf.getInt(
						DynamoDBConfiguration.HASH_KEY_TYPE_PROPERTY,
						Types.STRING.ordinal())).andReturn(type.ordinal());
		expect(conf.get(DynamoDBConfiguration.HASH_KEY_VALUE_PROPERTY))
				.andReturn(null);

		replay(conf);

		assertEquals(VALUE, DynamoDBQueryInputFormat.getHashKeyValue(conf)
				.getS());
		assertNull(DynamoDBQueryInputFormat.getHashKeyValue(conf));

		verify(conf);
	}

	@Test
	public void testGetRangeKeyType() {
		Configuration conf = createMock(Configuration.class);
		expect(
				conf.getInt(
						DynamoDBConfiguration.RANGE_KEY_TYPE_PROPERTY,
						Types.STRING.ordinal())).andReturn(
				Types.NUMBER.ordinal());

		replay(conf);
		Types type = DynamoDBQueryInputFormat.getRangeKeyType(conf);
		assertEquals(Types.NUMBER, type);
		verify(conf);
	}

	@Test
	public void testSetRangeKeyType() {
		Configuration conf = createMock(Configuration.class);

		conf.setInt(
				DynamoDBConfiguration.RANGE_KEY_TYPE_PROPERTY,
				Types.NUMBER.ordinal());

		replay(conf);
		DynamoDBQueryInputFormat.setRangeKeyType(conf, Types.NUMBER);
		verify(conf);
	}

	@Test
	public void testGetInterpolateAcrossRangeKeyValues() {
		Configuration conf = createMock(Configuration.class);
		expect(
				conf.getBoolean(
						DynamoDBConfiguration.RANGE_KEY_INTERPOLATE_PROPERTY,
						false)).andReturn(true);

		replay(conf);
		boolean result =
				DynamoDBQueryInputFormat
						.getInterpolateAcrossRangeKeyValues(conf);
		assertEquals(true, result);
		verify(conf);
	}

	@Test
	public void testSetInterpolateAcrossRangeKeyValues() {
		Configuration conf = createMock(Configuration.class);

		conf.setBoolean(
				DynamoDBConfiguration.RANGE_KEY_INTERPOLATE_PROPERTY,
				true);

		replay(conf);
		DynamoDBQueryInputFormat.setInterpolateAcrossRangeKeyValues(conf, true);
		verify(conf);
	}

	@Test
	public void testGetRangeKeyOperator() {
		Configuration conf = createMock(Configuration.class);
		expect(
				conf.getInt(
						DynamoDBConfiguration.RANGE_KEY_OPERATOR_PROPERTY,
						ComparisonOperator.EQ.ordinal())).andReturn(
				ComparisonOperator.BETWEEN.ordinal());

		replay(conf);
		ComparisonOperator operator =
				DynamoDBQueryInputFormat.getRangeKeyComparisonOperator(conf);
		assertEquals(ComparisonOperator.BETWEEN, operator);
		verify(conf);
	}

	@Test
	public void testSetRangeKeyOperator() {
		Configuration conf = createMock(Configuration.class);

		conf.setInt(
				DynamoDBConfiguration.RANGE_KEY_OPERATOR_PROPERTY,
				ComparisonOperator.BETWEEN.ordinal());

		replay(conf);
		DynamoDBQueryInputFormat.setRangeKeyComparisonOperator(
				conf,
				ComparisonOperator.BETWEEN);
		verify(conf);
	}

	@Test
	public void testSetRangeKeyValues() {
		Configuration conf = createMock(Configuration.class);
		final String[] VALUES = new String[] { "TEST1", "TEST2" };
		Types type = Types.STRING;

		List<AttributeValue> attrs = new ArrayList<AttributeValue>();
		for (String value : VALUES) {
			attrs.add(new AttributeValue().withS(value));
		}

		conf.setBoolean(
				DynamoDBConfiguration.RANGE_KEY_INTERPOLATE_PROPERTY,
				false);
		conf.setInt(
				DynamoDBConfiguration.RANGE_KEY_TYPE_PROPERTY,
				type.ordinal());
		conf.setStrings(DynamoDBConfiguration.RANGE_KEY_VALUES_PROPERTY, VALUES);

		replay(conf);

		DynamoDBQueryInputFormat.setRangeKeyValues(conf, type, attrs);

		verify(conf);
	}

	@Test
	public void testGetRangeKeyValues() {
		Configuration conf = createMock(Configuration.class);
		final String[] VALUES = new String[] { "TEST1", "TEST2" };
		Types type = Types.STRING;

		List<AttributeValue> attrs = new ArrayList<AttributeValue>();
		for (String value : VALUES) {
			attrs.add(new AttributeValue().withS(value));
		}

		expect(
				conf.getInt(
						DynamoDBConfiguration.RANGE_KEY_TYPE_PROPERTY,
						Types.STRING.ordinal())).andReturn(type.ordinal());
		expect(conf.getStrings(DynamoDBConfiguration.RANGE_KEY_VALUES_PROPERTY))
				.andReturn(VALUES);

		replay(conf);

		Collection<AttributeValue> results =
				DynamoDBQueryInputFormat.getRangeKeyValues(conf);
		int i = 0;
		for (AttributeValue result : results) {
			assertEquals(VALUES[i++], result.getS());
		}

		verify(conf);
	}

	@Test
	public void testSetRangeKeyCondition() {
		Configuration conf = createMock(Configuration.class);
		final String[] VALUES = new String[] { "TEST1", "TEST2" };
		Types type = Types.STRING;
		ComparisonOperator operator = ComparisonOperator.BETWEEN;

		List<AttributeValue> attrs = new ArrayList<AttributeValue>();
		for (String value : VALUES) {
			attrs.add(new AttributeValue().withS(value));
		}

		conf.setBoolean(
				DynamoDBConfiguration.RANGE_KEY_INTERPOLATE_PROPERTY,
				false);
		conf.setInt(
				DynamoDBConfiguration.RANGE_KEY_OPERATOR_PROPERTY,
				ComparisonOperator.BETWEEN.ordinal());
		conf.setInt(
				DynamoDBConfiguration.RANGE_KEY_TYPE_PROPERTY,
				type.ordinal());
		conf.setStrings(DynamoDBConfiguration.RANGE_KEY_VALUES_PROPERTY, VALUES);

		replay(conf);

		DynamoDBQueryInputFormat.setRangeKeyCondition(
				conf,
				type,
				operator,
				attrs);

		verify(conf);
	}

	@Test
	public void testInputSplitReadAndWriteHashKey() throws IOException {
		String HASH_KEY_STRING = "TEST";
		String RANGE_KEY_NUMBER = "007";

		// configure input split
		Types hashKeyType = Types.STRING;
		AttributeValue hashKeyValue =
				new AttributeValue().withS(HASH_KEY_STRING);
		Types rangeKeyType = Types.NUMBER;
		Collection<AttributeValue> rangeKeyValues =
				new ArrayList<AttributeValue>();
		rangeKeyValues.add(new AttributeValue().withN(RANGE_KEY_NUMBER));
		ComparisonOperator rangeKeyOpperator = ComparisonOperator.EQ;

		DynamoDBQueryInputFormat.DynamoDBQueryInputSplit inputSplit =
				new DynamoDBQueryInputFormat.DynamoDBQueryInputSplit(
						hashKeyType,
						hashKeyValue,
						rangeKeyType,
						rangeKeyValues,
						rangeKeyOpperator);

		// write values to byte array
		ByteArrayOutputStream bout = new ByteArrayOutputStream();
		DataOutputStream out = new DataOutputStream(bout);
		inputSplit.write(out);

		out.flush();

		// read values back in from byte array
		ByteArrayInputStream bin = new ByteArrayInputStream(bout.toByteArray());
		DataInputStream in = new DataInputStream(bin);
		DynamoDBQueryInputFormat.DynamoDBQueryInputSplit readSplit =
				new DynamoDBQueryInputFormat.DynamoDBQueryInputSplit();
		readSplit.readFields(in);

		// verify loaded values
		assertEquals(hashKeyType, readSplit.getHashKeyType());
		assertEquals(HASH_KEY_STRING, readSplit.getHashKeyValue().getS());
		assertEquals(rangeKeyType, readSplit.getRangeKeyType());
		assertEquals(rangeKeyOpperator, readSplit.getRangeKeyOperator());
		assertArrayEquals(rangeKeyValues.toArray(), readSplit
				.getRangeKeyValues().toArray());
		assertTrue(readSplit.hasRangeKey());
	}

	@Test
	public void testGetRangeKeyInterpolateMinValue() {
		Configuration conf = createMock(Configuration.class);
		final String VALUE = "TEST";
		Types type = Types.STRING;

		expect(
				conf.getInt(
						DynamoDBConfiguration.RANGE_KEY_TYPE_PROPERTY,
						Types.STRING.ordinal())).andReturn(type.ordinal());
		expect(
				conf.get(DynamoDBConfiguration.RANGE_KEY_INTERPOLATE_MIN_VALUE_PROPERTY))
				.andReturn(VALUE);

		replay(conf);

		AttributeValue attr =
				DynamoDBQueryInputFormat.getRangeKeyInterpolateMinValue(conf);
		assertEquals(VALUE, attr.getS());

		verify(conf);
	}

	@Test
	public void testSetRangeKeyInterpolateMinValue() {
		Configuration conf = createMock(Configuration.class);
		final String VALUE = "TEST";
		Types type = Types.STRING;
		AttributeValue attr = new AttributeValue().withS(VALUE);

		conf.setBoolean(
				DynamoDBConfiguration.RANGE_KEY_INTERPOLATE_PROPERTY,
				true);
		conf.setInt(
				DynamoDBConfiguration.RANGE_KEY_TYPE_PROPERTY,
				type.ordinal());
		conf.set(
				DynamoDBConfiguration.RANGE_KEY_INTERPOLATE_MIN_VALUE_PROPERTY,
				VALUE);

		replay(conf);

		DynamoDBQueryInputFormat.setRangeKeyInterpolateMinValue(
				conf,
				type,
				attr);

		verify(conf);
	}

	@Test
	public void testGetRangeKeyInterpolateMaxValue() {
		Configuration conf = createMock(Configuration.class);
		final String VALUE = "TEST";
		Types type = Types.STRING;

		expect(
				conf.getInt(
						DynamoDBConfiguration.RANGE_KEY_TYPE_PROPERTY,
						Types.STRING.ordinal())).andReturn(type.ordinal());
		expect(
				conf.get(DynamoDBConfiguration.RANGE_KEY_INTERPOLATE_MAX_VALUE_PROPERTY))
				.andReturn(VALUE);

		replay(conf);

		AttributeValue attr =
				DynamoDBQueryInputFormat.getRangeKeyInterpolateMaxValue(conf);
		assertEquals(VALUE, attr.getS());

		verify(conf);
	}

	@Test
	public void testSetRangeKeyInterpolateMaxValue() {
		Configuration conf = createMock(Configuration.class);
		final String VALUE = "TEST";
		Types type = Types.STRING;
		AttributeValue attr = new AttributeValue().withS(VALUE);

		conf.setBoolean(
				DynamoDBConfiguration.RANGE_KEY_INTERPOLATE_PROPERTY,
				true);
		conf.setInt(
				DynamoDBConfiguration.RANGE_KEY_TYPE_PROPERTY,
				type.ordinal());
		conf.set(
				DynamoDBConfiguration.RANGE_KEY_INTERPOLATE_MAX_VALUE_PROPERTY,
				VALUE);

		replay(conf);

		DynamoDBQueryInputFormat.setRangeKeyInterpolateMaxValue(
				conf,
				type,
				attr);

		verify(conf);
	}

	@Test
	public void testSetRangeKeyInterpolateRange() {
		Configuration conf = createMock(Configuration.class);
		final String MIN_VALUE = "TEST1";
		final String MAX_VALUE = "TEST2";
		Types type = Types.STRING;

		AttributeValue min_attr = new AttributeValue().withS(MIN_VALUE);
		AttributeValue max_attr = new AttributeValue().withS(MAX_VALUE);

		conf.setBoolean(
				DynamoDBConfiguration.RANGE_KEY_INTERPOLATE_PROPERTY,
				true);
		conf.setInt(
				DynamoDBConfiguration.RANGE_KEY_TYPE_PROPERTY,
				type.ordinal());
		conf.set(
				DynamoDBConfiguration.RANGE_KEY_INTERPOLATE_MIN_VALUE_PROPERTY,
				MIN_VALUE);

		conf.setBoolean(
				DynamoDBConfiguration.RANGE_KEY_INTERPOLATE_PROPERTY,
				true);
		conf.setInt(
				DynamoDBConfiguration.RANGE_KEY_TYPE_PROPERTY,
				type.ordinal());
		conf.set(
				DynamoDBConfiguration.RANGE_KEY_INTERPOLATE_MAX_VALUE_PROPERTY,
				MAX_VALUE);

		replay(conf);

		DynamoDBQueryInputFormat.setRangeKeyInterpolateRange(
				conf,
				type,
				min_attr,
				max_attr);

		verify(conf);

	}

}
