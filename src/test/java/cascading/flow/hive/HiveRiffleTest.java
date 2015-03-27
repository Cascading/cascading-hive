/*
* Copyright (c) 2007-2015 Concurrent, Inc. All Rights Reserved.
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

package cascading.flow.hive;


import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import cascading.CascadingException;
import cascading.HiveTestCase;
import cascading.scheme.NullScheme;
import cascading.tap.Tap;
import cascading.tap.hadoop.Hfs;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.CommandNeedRetryException;
import org.apache.hadoop.hive.ql.Driver;
import org.apache.hadoop.hive.ql.processors.CommandProcessorResponse;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.RecordReader;
import org.junit.Test;
import org.mockito.Mockito;

import static org.mockito.Mockito.*;


/**
 * Integration style tests, which bootstrap a set of tables and use them from cascading later on.
 */
public class HiveRiffleTest extends HiveTestCase
  {

  @Test(expected = CascadingException.class)
  public void testCompleteProblematicQuery()
    {
    Tap mockSource = Mockito.mock( Tap.class );
    Tap mockSink = Mockito.mock( Tap.class );
    HiveRiffle riffle = new HiveRiffle( createHiveDriverFactory(), new String[]{"creat table foo (key string)"},
      Arrays.asList( mockSource ), mockSink );
    riffle.complete();
    }

  @Test(expected = CascadingException.class)
  public void testEmptySources()
    {
    Tap mockSink = Mockito.mock( Tap.class );
    new HiveRiffle( createHiveDriverFactory(), new String[]{"create table foo (key string)"},
      new ArrayList<Tap>(), mockSink );
    }

  @Test(expected = CascadingException.class)
  public void testNullSources()
    {
    Tap mockSink = Mockito.mock( Tap.class );
    new HiveRiffle( createHiveDriverFactory(), new String[]{"create table foo (key string)"},
      null, mockSink );
    }

  @Test
  public void testIncomingAndOutgoing()
    {
    List<Tap> incoming = new ArrayList<Tap>();
    incoming.add( new Hfs( new NullScheme<Configuration, RecordReader, OutputCollector, Object, Object>(), "/foo/bar" ) );
    incoming.add( new Hfs( new NullScheme<Configuration, RecordReader, OutputCollector, Object, Object>(), "/quux/bla" ) );
    Tap outgoing = new Hfs( new NullScheme<Configuration, RecordReader, OutputCollector, Object, Object>(), "/out" );

    HiveRiffle riffle = new HiveRiffle( createHiveDriverFactory(), new String[]{"create table foo (key string)"},
      incoming, outgoing );
    assertEquals( incoming, riffle.getIncoming() );
    assertEquals( 1, riffle.getOutgoing().size() );
    assertEquals( outgoing, riffle.getOutgoing().iterator().next() );
    }

  @Test(expected = CascadingException.class)
  public void testCompleteWithRetryException() throws Exception
    {
    Driver mockDriver = mock( Driver.class );
    when( mockDriver.run( anyString() ) ).thenThrow( new CommandNeedRetryException( "testing" ) );
    HiveDriverFactory factory = mock( HiveDriverFactory.class );
    when( factory.createHiveDriver() ).thenReturn( mockDriver );

    List<Tap> incoming = new ArrayList<Tap>();
    incoming.add( new Hfs( new NullScheme<Configuration, RecordReader, OutputCollector, Object, Object>(), "/foo/bar" ) );
    incoming.add( new Hfs( new NullScheme<Configuration, RecordReader, OutputCollector, Object, Object>(), "/quux/bla" ) );
    Tap outgoing = new Hfs( new NullScheme<Configuration, RecordReader, OutputCollector, Object, Object>(), "/out" );

    HiveRiffle riffle = new HiveRiffle( factory, new String[]{"creat table foo (key string)"},
      incoming, outgoing );
    riffle.complete();
    verify( mockDriver ).destroy();
    }

  @Test(expected = CascadingException.class)
  public void testCompleteWithProblematicStatement() throws Exception
    {
    Driver mockDriver = mock( Driver.class );
    when( mockDriver.run( anyString() ) ).thenReturn( new CommandProcessorResponse( -1, "test error", null ) );
    HiveDriverFactory factory = mock( HiveDriverFactory.class );
    when( factory.createHiveDriver() ).thenReturn( mockDriver );

    List<Tap> incoming = new ArrayList<Tap>();
    incoming.add( new Hfs( new NullScheme<Configuration, RecordReader, OutputCollector, Object, Object>(), "/foo/bar" ) );
    incoming.add( new Hfs( new NullScheme<Configuration, RecordReader, OutputCollector, Object, Object>(), "/quux/bla" ) );
    Tap outgoing = new Hfs( new NullScheme<Configuration, RecordReader, OutputCollector, Object, Object>(), "/out" );

    HiveRiffle riffle = new HiveRiffle( factory, new String[]{"creat table foo (key string)"},
      incoming, outgoing );
    riffle.complete();
    verify( mockDriver ).destroy();
    }

  @Test
  public void testCompleteHappyCase() throws Exception
    {
    int ok = 0;
    Driver mockDriver = mock( Driver.class );
    when( mockDriver.run( anyString() ) ).thenReturn( new CommandProcessorResponse( ok, "test error", null ) );
    HiveDriverFactory factory = mock( HiveDriverFactory.class );
    when( factory.createHiveDriver() ).thenReturn( mockDriver );

    List<Tap> incoming = new ArrayList<Tap>();
    incoming.add( new Hfs( new NullScheme<Configuration, RecordReader, OutputCollector, Object, Object>(), "/foo/bar" ) );
    incoming.add( new Hfs( new NullScheme<Configuration, RecordReader, OutputCollector, Object, Object>(), "/quux/bla" ) );
    Tap outgoing = new Hfs( new NullScheme<Configuration, RecordReader, OutputCollector, Object, Object>(), "/out" );

    HiveRiffle riffle = new HiveRiffle( factory, new String[]{"creat table foo (key string)"},
      incoming, outgoing );
    riffle.complete();
    verify( mockDriver ).destroy();
    }

  @Test
  public void testGetCounters() throws Exception
    {
    HiveDriverFactory factory = new HiveDriverFactory();

    List<Tap> incoming = new ArrayList<Tap>();
    incoming.add( new Hfs( new NullScheme<Configuration, RecordReader, OutputCollector, Object, Object>(), "/foo/bar" ) );
    incoming.add( new Hfs( new NullScheme<Configuration, RecordReader, OutputCollector, Object, Object>(), "/quux/bla" ) );
    Tap outgoing = new Hfs( new NullScheme<Configuration, RecordReader, OutputCollector, Object, Object>(), "/out" );

    HiveRiffle riffle = new HiveRiffle( factory, new String[]{"create table counters (key string)"},
      incoming, outgoing );
    riffle.complete();
    assertEquals( 0, riffle.getCounters().size() );
    }

  @Test(expected = RuntimeException.class)
  public void testCompleteDriverCreationFailure() throws Exception
    {
    HiveDriverFactory factory = mock( HiveDriverFactory.class );
    when( factory.createHiveDriver() ).thenThrow( new RuntimeException( "testing" ) );

    List<Tap> incoming = new ArrayList<Tap>();
    incoming.add( new Hfs( new NullScheme<Configuration, RecordReader, OutputCollector, Object, Object>(), "/foo/bar" ) );
    incoming.add( new Hfs( new NullScheme<Configuration, RecordReader, OutputCollector, Object, Object>(), "/quux/bla" ) );
    Tap outgoing = new Hfs( new NullScheme<Configuration, RecordReader, OutputCollector, Object, Object>(), "/out" );

    HiveRiffle riffle = new HiveRiffle( factory, new String[]{"creat table foo (key string)"},
      incoming, outgoing );
    riffle.complete();
    }

  }
