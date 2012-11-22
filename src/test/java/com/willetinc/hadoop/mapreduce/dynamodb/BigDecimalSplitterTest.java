package com.willetinc.hadoop.mapreduce.dynamodb;

import static org.easymock.EasyMock.*;
import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputSplit;
import org.junit.Test;

import com.amazonaws.services.dynamodb.model.AttributeValue;
import com.willetinc.hadoop.mapreduce.dynamodb.BigDecimalSplitter;
import com.willetinc.hadoop.mapreduce.dynamodb.DynamoDBQueryInputFormat;
import com.willetinc.hadoop.mapreduce.dynamodb.Types;

public class BigDecimalSplitterTest {

	@Test
	public void testSplitInteger() {

		final int NUM_RANGE_SPLITS = 2;
		final String VALUE = "007";
		final Types hashKeyType = Types.NUMBER;
		final AttributeValue hashKeyValue = new AttributeValue().withN(VALUE);
		final Types rangeKeyType = Types.NUMBER;
		final AttributeValue minRangeKeyValue =
				new AttributeValue().withN("0.0");
		final AttributeValue maxRangeKeyValue =
				new AttributeValue().withN("100.0");

		Configuration conf = createMock(Configuration.class);
		BigDecimalSplitter splitter = new BigDecimalSplitter();

		List<InputSplit> inputSplits = new ArrayList<InputSplit>();

		splitter.generateRangeKeySplits(
				conf,
				inputSplits,
				hashKeyType,
				hashKeyValue,
				rangeKeyType,
				minRangeKeyValue,
				maxRangeKeyValue,
				NUM_RANGE_SPLITS);

		assertEquals(2, inputSplits.size());

		DynamoDBQueryInputFormat.DynamoDBQueryInputSplit split1 =
				(DynamoDBQueryInputFormat.DynamoDBQueryInputSplit) inputSplits
						.get(0);
		Iterator<AttributeValue> itr1 = split1.getRangeKeyValues().iterator();

		System.out.println(itr1.next());
		System.out.println(split1.getRangeKeyOperator());
		System.out.println(itr1.next());

		DynamoDBQueryInputFormat.DynamoDBQueryInputSplit split2 =
				(DynamoDBQueryInputFormat.DynamoDBQueryInputSplit) inputSplits
						.get(1);
		Iterator<AttributeValue> itr2 = split2.getRangeKeyValues().iterator();

		System.out.print("BETWEEN " + split2.getRangeKeyOperator());
		System.out.print(itr2.next() + " AND ");
		System.out.println(itr2.next());

	}

}
