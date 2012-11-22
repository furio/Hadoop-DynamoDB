Hadoop-DynamoDB
===============

This library provides [InputFormats](http://hadoop.apache.org/docs/current/api/org/apache/hadoop/mapred/InputFormat.html), [OutputFormats](http://hadoop.apache.org/docs/current/api/org/apache/hadoop/mapred/OutputFormat.html), and [Writable](http://hadoop.apache.org/docs/current/api/org/apache/hadoop/io/Writable.html) classes for reading and writing data to [Amazon DynamoDB](http://aws.amazon.com/dynamodb/) tables. 


DynamoDB Writables
------------------

Provided are Writable implementations for the six [DynamoDB Data Types](http://docs.amazonwebservices.com/amazondynamodb/latest/developerguide/DataModel.html#DataModelDataTypes). These classes are all abstract as Hadoop requires that Writable implementations must have a default constructor. 

### Writable Implementations:
 - NWritable
 - SWritable
 - BWritable
 - NSWritable
 - SSWritable
 - BSWritable

### Defining Columns
To create a column extend one of these calsses and provide the column name to the super() constructor.
```java
public class MyColumn extends SWritable {

    public MyColumn() {
        super("column-name");
    }

}
```

### Defining Tables
To create a table extend DynamoDBItemWritable and pass a list of columns into the super() constructor. The first parameter must be the HashKey, the second the RangeKey, followed by any number of optional additional columns. To specify a table with only a HashKey simply pass in a null value for the RangeKey argument.

```java
public class MyTable extends DynamoDBItemWritable {

    public static final NWritable NUMBER_COLUMN = new NWritable("n-fieldname") {};
    public static final SWritable STRING_COLUMN = new SWritable("s-fieldname") {};
    public static final BWritable BINARY_COLUMN = new BWritable("b-fieldname") {};

    public static final NSWritable NUM_SET_COLUMN = new NSWritable("ns-fieldname") {};
    public static final SSWritable STRING_SET_COLUMN = new SSWritable("ss-fieldname") {};
    public static final BSWritable BINARY_SET_COLUMN = new BSWritable("bs-fieldname") {};

    public MyTable() {
        super(
            NUMBER_COLUMN, // HashKey
            STRING_COLUMN, // RangeKey or null
            BINARY_COLUMN, // remaining column definitions
            NUM_SET_COLUMN, 
            STRING_SET_COLUMN, 
            BINARY_SET_COLUMN);
    }
	
}
```
### Defining HashKey Table
```java
public static final DynamoDBItemWritable MY_TABLE = new DynamoDBItemWritable(
	new NWritable("hashkey-column-name") {}, // HashKey
	null									 // range key is null
) {};
```

### Defining a HashKey and RangeKey
```java
public static final DynamoDBItemWritable MY_TABLE = new DynamoDBItemWritable(
	new NWritable("hashkey-column-name") {},  // HashKey
	new NWritable("rangekey-column-name") {}  // RangeKey
) {};
```

License
-------

Copyright 2012 Willet Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.