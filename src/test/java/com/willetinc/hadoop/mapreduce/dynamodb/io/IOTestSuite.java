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

import org.junit.runner.RunWith;
import org.junit.runners.Suite;

import com.willetinc.hadoop.mapreduce.dynamodb.io.AttributeValueWritableTest;
import com.willetinc.hadoop.mapreduce.dynamodb.io.BSWritableTest;
import com.willetinc.hadoop.mapreduce.dynamodb.io.BWritableTest;
import com.willetinc.hadoop.mapreduce.dynamodb.io.NSWritableTest;
import com.willetinc.hadoop.mapreduce.dynamodb.io.NWritableTest;
import com.willetinc.hadoop.mapreduce.dynamodb.io.SSWritableTest;
import com.willetinc.hadoop.mapreduce.dynamodb.io.SWritableTest;

@RunWith(Suite.class)
@Suite.SuiteClasses({
		AttributeValueWritableTest.class,
		BWritableTest.class,
		BSWritableTest.class,
		NWritableTest.class,
		NSWritableTest.class,
		SWritableTest.class,
		SSWritableTest.class})
public class IOTestSuite {
	// nothing
}
