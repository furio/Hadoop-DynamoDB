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

package com.willetinc.hadoop.mapreduce.dynamodb.io;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.willetinc.hadoop.mapreduce.dynamodb.Types;

public abstract class BWritable extends AttributeValueWritable {

	public BWritable(String fieldName) {
		super(Types.BINARY, fieldName);
	}

	public BWritable(String fieldName, AttributeValue value) {
		super(Types.BINARY, fieldName, value);
	}

}
