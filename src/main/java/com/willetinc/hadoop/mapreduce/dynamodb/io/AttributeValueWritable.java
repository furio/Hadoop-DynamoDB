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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.willetinc.hadoop.mapreduce.dynamodb.AttributeValueIOUtils;
import com.willetinc.hadoop.mapreduce.dynamodb.Types;

public abstract class AttributeValueWritable implements Writable {
	
	private final String fieldName;
	
	private final Types type;
	
	private AttributeValue value;
	
	public AttributeValueWritable(Types type, String fieldName) {
		this.type = type;
		this.fieldName = fieldName;
	}
	
	public AttributeValueWritable(Types type, String fieldName, AttributeValue value) {
		this.type = type;
		this.fieldName = fieldName;
		this.value = value;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		value = AttributeValueIOUtils.read(type, in);

	}

	@Override
	public void write(DataOutput out) throws IOException {
		AttributeValueIOUtils.write(type, value, out);
	}

	public String getFieldName() {
		return fieldName;
	}

	public Types getType() {
		return type;
	}

	public AttributeValue getValue() {
		return value;
	}

	public void setValue(AttributeValue value) {
		this.value = value;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result =
				prime * result
						+ ((fieldName == null) ? 0 : fieldName.hashCode());
		result = prime * result + ((type == null) ? 0 : type.hashCode());
		result = prime * result + ((value == null) ? 0 : value.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		AttributeValueWritable other = (AttributeValueWritable) obj;
		if (fieldName == null) {
			if (other.fieldName != null)
				return false;
		} else if (!fieldName.equals(other.fieldName))
			return false;
		if (type != other.type)
			return false;
		if (value == null) {
			if (other.value != null)
				return false;
		} else if (!value.equals(other.value))
			return false;
		return true;
	}
}
