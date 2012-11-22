package com.willetinc.hadoop.mapreduce.dynamodb;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import com.amazonaws.services.dynamodb.model.AttributeValue;

public abstract class DynamoDBItemWritable implements DynamoDBKeyWritable {
	
	private final AttributeValueWritable hashKey;
	
	private final AttributeValueWritable rangeKey;
	
	private final AttributeValueWritable[] fields;
	
	protected DynamoDBItemWritable(AttributeValueWritable hashKey, AttributeValueWritable rangeKey, AttributeValueWritable...fields) {
		this.hashKey = hashKey;
		this.rangeKey = rangeKey;
		
		if(null == rangeKey) {
			this.fields = new AttributeValueWritable[fields.length+1];
		} else {
			// has RangeKey
			this.fields = new AttributeValueWritable[fields.length+2];
			this.fields[1] = rangeKey;
		}
		this.fields[0] = hashKey;

		// copy remaining fields
		System.arraycopy(fields, 0, this.fields, 2, fields.length);
	}
	
	@Override
	public AttributeValueWritable getHashKey() {
		return hashKey;
	}
	
	@Override
	public AttributeValue getHashKeyValue() {
		return hashKey.getValue();
	}
	
	@Override
	public void setHashKeyValue(AttributeValue hashKey) {
		this.hashKey.setValue(hashKey);
	}
	
	@Override
	public boolean hasRangeKey() {
		return (null == rangeKey);
	}
	
	@Override
	public AttributeValueWritable getRangeKey() {
		return rangeKey;
	}

	@Override
	public AttributeValue getRangeKeyValue() {
		return rangeKey.getValue();
	}
	
	@Override
	public void setRangeKeyValue(AttributeValue rangeKey) {
		this.rangeKey.setValue(rangeKey);
	}

	public AttributeValue get(int columnIndex) {
		return fields[columnIndex].getValue();
	}
	
	public void set(int columnIndex, AttributeValue val) {
		fields[columnIndex].setValue(val);
	}
	
	public String getString(int columnIndex) {
		initializeIfNecessary(columnIndex);
		return fields[columnIndex].getValue().getS();
	}
	
	public void setString(int columnIndex, String val) {
		initializeIfNecessary(columnIndex);
		fields[columnIndex].getValue().setS(val);
	}
	
	public String getNumber(int columnIndex) {
		initializeIfNecessary(columnIndex);
		return fields[columnIndex].getValue().getN();
	}
	
	public void setNumber(int columnIndex, String val) {
		initializeIfNecessary(columnIndex);
		fields[columnIndex].getValue().setN(val);
	}
	
	public ByteBuffer getByteBuffer(int columnIndex) {
		return fields[columnIndex].getValue().getB();
	}
	
	public void setByteBuffer(int columnIndex, ByteBuffer buf) {
		initializeIfNecessary(columnIndex);
		fields[columnIndex].getValue().setB(buf);
	}
	
	public List<String> getStringSet(int columnIndex) {
		initializeIfNecessary(columnIndex);
		return fields[columnIndex].getValue().getSS();
	}
	
	public void setStringSet(int columnIndex, Collection<String> vals) {
		initializeIfNecessary(columnIndex);
		fields[columnIndex].getValue().setSS(vals);
	}
	
	public List<String> getNumberSet(int columnIndex) {
		initializeIfNecessary(columnIndex);
		return fields[columnIndex].getValue().getNS();
	}
	
	public void setNumber(int columnIndex, Collection<String> vals) {
		initializeIfNecessary(columnIndex);
		fields[columnIndex].getValue().setNS(vals);
	}
	
	public List<ByteBuffer> getByteBufferSet(int columnIndex) {
		return fields[columnIndex].getValue().getBS();
	}
	
	public void setByteBufferSet(int columnIndex, Collection<ByteBuffer> buf) {
		initializeIfNecessary(columnIndex);
		fields[columnIndex].getValue().setBS(buf);
	}
	
	private void initializeIfNecessary(int columnIndex) {
		if(null == fields[columnIndex].getValue()) {
			fields[columnIndex].setValue(new AttributeValue());
		}
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		for(AttributeValueWritable field: fields) {
			field.readFields(in);
		}
	}

	@Override
	public void write(DataOutput out) throws IOException {
		for(AttributeValueWritable field: fields) {
			field.write(out);
		}
	}

	@Override
	public void readFields(Map<String, AttributeValue> in) {
		for(AttributeValueWritable field: fields) {
			field.setValue(in.get(field.getFieldName()));
		}
		
	}

	@Override
	public void write(Map<String, AttributeValue> out) {
		for(AttributeValueWritable field: fields) {
			out.put(field.getFieldName(), field.getValue());
		}
	}
	
	
}
