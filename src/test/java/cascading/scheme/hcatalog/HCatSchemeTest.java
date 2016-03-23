/*
* Copyright (c) 2007-2016 Concurrent, Inc. All Rights Reserved.
*
* Project and contact information: http://www.cascading.org/
*
* This file is part of the Cascading project.
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

package cascading.scheme.hcatalog;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;

import cascading.flow.FlowProcess;
import cascading.scheme.SinkCall;
import cascading.scheme.SourceCall;
import cascading.scheme.hcatalog.HCatScheme.SourceContext;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hive.hcatalog.common.HCatException;
import org.apache.hive.hcatalog.data.DefaultHCatRecord;
import org.apache.hive.hcatalog.data.HCatRecord;
import org.apache.hive.hcatalog.data.schema.HCatSchema;
import org.apache.hive.hcatalog.data.schema.HCatSchemaUtils;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import static org.junit.Assert.*;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class HCatSchemeTest
  {

  private
  @Mock
  FlowProcess<? extends Configuration> flowProcess;
  private
  @Mock
  SourceCall<SourceContext, RecordReader> sourceCall;
  private
  @Mock
  RecordReader recordReader;
  private
  @Mock
  SinkCall<HCatSchema, OutputCollector> sinkCall;
  private
  @Mock
  OutputCollector outputCollector;
  private HCatSchema schema;

  @Before
  public void init() throws HCatException
    {
    List<FieldSchema> fieldSchemas = new ArrayList<>( 3 );
    fieldSchemas.add( new FieldSchema( "foo", "string", null ) );
    fieldSchemas.add( new FieldSchema( "bar", "int", null ) );
    fieldSchemas.add( new FieldSchema( "baz", "int", null ) );
    fieldSchemas.add( new FieldSchema( "zap", "string", null ) );
    schema = HCatSchemaUtils.getHCatSchema( fieldSchemas );
    }

  @Test(expected = IllegalArgumentException.class)
  public void duplicateFields()
    {
    new HCatScheme( new Fields( "FOO", "foo" ) );
    }

  @Test
  public void source() throws IOException
    {
    NullWritable key = NullWritable.get();
    HCatRecord record = new DefaultHCatRecord( 4 );
    record.set( 0, "I am foo" );
    record.set( 1, 4 );
    record.set( 2, 6 );
    record.set( 3, "I am zap" );
    SourceContext context = new SourceContext( schema, key, record );
    when( sourceCall.getContext() ).thenReturn( context );

    when( recordReader.next( any(), any() ) ).thenReturn( true );

    Fields fields = new Fields( new String[]{"zap", "foo", "baz", "bar"},
      new Type[]{String.class, String.class, Integer.class, Integer.class} );
    TupleEntry tupleEntry = new TupleEntry( fields, new Tuple( null, null, null, null ) );
    when( sourceCall.getIncomingEntry() ).thenReturn( tupleEntry );
    when( sourceCall.getInput() ).thenReturn( recordReader );

    HCatScheme scheme = new HCatScheme( fields );
    assertTrue( scheme.source( flowProcess, sourceCall ) );

    assertEquals( "I am foo", tupleEntry.getString( "foo" ) );
    assertEquals( 4, tupleEntry.getInteger( "bar" ) );
    assertEquals( 6, tupleEntry.getInteger( "baz" ) );
    assertEquals( "I am zap", tupleEntry.getString( "zap" ) );
    }

  @Test
  public void sink() throws IOException
    {
    when( sinkCall.getContext() ).thenReturn( schema );

    Fields fields = new Fields( new String[]{"bar", "foo", "zap"},
      new Type[]{Integer.class, String.class, String.class} );
    Tuple tuple = new Tuple( 1, "I am foo", "I am zap" );
    when( sinkCall.getOutgoingEntry() ).thenReturn( new TupleEntry( fields, tuple ) );

    when( sinkCall.getOutput() ).thenReturn( outputCollector );

    ArgumentCaptor<Object> outputKey = ArgumentCaptor.forClass( Object.class );
    ArgumentCaptor<Object> outputValue = ArgumentCaptor.forClass( Object.class );

    HCatScheme scheme = new HCatScheme( fields );
    scheme.sink( flowProcess, sinkCall );

    verify( outputCollector ).collect( outputKey.capture(), outputValue.capture() );
    assertNull( outputKey.getValue() );
    HCatRecord output = (HCatRecord) outputValue.getValue();
    assertNotNull( output );
    assertEquals( 4, output.size() );
    assertEquals( "I am foo", output.get( 0 ) );
    assertEquals( 1, output.get( 1 ) );
    assertNull( output.get( 2 ) );
    assertEquals( "I am zap", output.get( 3 ) );
    }

  }
