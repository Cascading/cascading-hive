/*
* Copyright (c) 2007-2014 Concurrent, Inc. All Rights Reserved.
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


import java.io.IOException;
import java.util.Map;
import java.util.Properties;

import cascading.CascadingException;
import cascading.HiveTestCase;
import cascading.cascade.CascadeConnector;
import cascading.tap.Tap;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.ql.Driver;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.junit.Test;


/**
 * Integration style tests, which bootstrap a set of tables and use them from cascading later on.
 */
public class HiveFlowTest extends HiveTestCase
  {
  /*
  @Test(expected = CascadingException.class)
  public void testWrongSQL()
    {
    new HiveFlow( "broken", new Properties(), "create tble foobar", createHiveConf() );
    }
   */

  @Test
  public void testExistingTable() throws Exception
    {
    SessionState.start( createHiveConf() );
    SessionState.get().setIsSilent( true );
    Driver driver = new Driver( createHiveConf() );
    driver.init();
    driver.run( "create table duplicate( value string ) row format delimited fields terminated by '\\t' " );
    driver.destroy();

    HiveMetaStoreClient client = new HiveMetaStoreClient( createHiveConf() );
    Table table = client.getTable( "default", "duplicate" );
    System.out.println(table);

    }

  /*
  @Test
  public void testCreateDualAndLoadDataIntoTable() throws IOException
    {
    HiveFlow createFlow = new HiveFlow( "create table", new Properties(), "create table dual( value string )", createHiveConf() );

    assertTrue( createFlow.stepsAreLocal() );
    CascadeConnector connector = new CascadeConnector();
    connector.connect( createFlow ).complete();

    Tap sink = createFlow.getSink();
    assertEquals( HIVE_WAREHOUSE_DIR + "/dual", sink.getIdentifier() );

    assertSame( HiveNullTap.DEV_ZERO, createFlow.getSources().values().iterator().next() );

    String dualTableInputFile = CWD + "/src/test/resources/data.txt";

    HiveFlow loadDataFlow = new HiveFlow( "load data", new Properties(),
      String.format( "load data local inpath '%s' overwrite into table dual ", dualTableInputFile ),
      createHiveConf() );
    assertEquals( HIVE_WAREHOUSE_DIR + "/dual", loadDataFlow.getSink().getIdentifier().substring( 5 ) );
    assertEquals( 1, loadDataFlow.getSources().size() );
    assertEquals( dualTableInputFile, loadDataFlow.getSources().values().iterator().next().getIdentifier().substring( 5 ) );


    HiveFlow createSecondTableFlow = new HiveFlow( "create second table", new Properties(),
      " create table keyvalue (key int, value string) row format delimited fields terminated by '\t' ",
      createHiveConf() );
    connector.connect( createSecondTableFlow ).complete();

    //TODO this does currently not work in a test, since hive insists on submitting jobs via the hadoop command and the
    // classpath detection is not right.


       /* HiveFlow selectFlow = new HiveFlow( "select data ", new Properties(),
       "insert into table keyvalue select 42 as key, 'awesome' as value from dual ",
        createHiveConf());

    Tap inTap = new Hfs( new TextLine(), selectFlow.getSink().getIdentifier() );
    Tap outTap = new Hfs( new TextLine(), CASCADING_OUTPUT_DIR + "/copy" );
    Pipe copyPipe = new Pipe( "copy" );

    Flow pureCascadingFlow = new HadoopFlowConnector().connect( inTap, outTap, copyPipe );
    Cascade cascade = connector.connect( pureCascadingFlow, loadDataFlow, selectFlow );
    cascade.complete();

    TupleEntryIterator iterator = pureCascadingFlow.openSink();
    assertTrue( iterator.hasNext() );

    TupleEntry te = iterator.next();

    assertEquals( 2, te.getTuple().size() ); */
 /*
    }

  @Test
  public void testJoin() throws IOException
    {
    CascadeConnector connector = new CascadeConnector();

    HiveFlow createFirstTableFlow = new HiveFlow( "create keyvalue table", new Properties(),
      " create table keyvalue3 (key int, value string) row format delimited fields terminated by '\t' ",
      createHiveConf() );

    HiveFlow createSecondTableFlow = new HiveFlow( "create keyvalue2 table", new Properties(),
      " create table keyvalue4 (key int, value string) row format delimited fields terminated by '\t' ",
      createHiveConf() );
    connector.connect( createFirstTableFlow, createSecondTableFlow ).complete();

    HiveFlow multitableSelect = new HiveFlow( "select from multiple tables", new Properties(),
      "select keyvalue3.key, keyvalue3.value from keyvalue3 join keyvalue4 on keyvalue3.key = keyvalue4.key",
      createHiveConf() );

    Map<String, Tap> sources = multitableSelect.getSources();
    assertEquals( 2, sources.size() );
    for ( String key: sources.keySet() )
      {
      Tap source = sources.get( key );
      assertNotNull ( source );
      assertEquals( key, source.getIdentifier() );
      }

    }


   */
  }
