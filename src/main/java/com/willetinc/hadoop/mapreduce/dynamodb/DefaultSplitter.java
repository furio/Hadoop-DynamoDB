package com.willetinc.hadoop.mapreduce.dynamodb;

import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputSplit;

import com.amazonaws.services.dynamodb.model.AttributeValue;

public class DefaultSplitter extends AbstractSplitter {

	private static final Log LOG = LogFactory.getLog(BigDecimalSplitter.class);
	
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
		
		LOG.error(String.format("Unable to generate RangeKey splits for RangeKeyType: %s", rangeKeyType));
	}

}
