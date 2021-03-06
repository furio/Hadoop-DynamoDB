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
