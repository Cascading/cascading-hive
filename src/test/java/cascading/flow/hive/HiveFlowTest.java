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


import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.Properties;

import cascading.CascadingException;
import cascading.PlatformTestCase;
import cascading.cascade.CascadeConnector;
import cascading.tap.Tap;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.junit.Before;
import org.junit.Test;


/**
 * Integration style tests, which bootstrap a set of tables and use them from cascading later on.
 */
public class HiveFlowTest extends PlatformTestCase
  {
  private final static File DERBY_HOME = new File( "build/test/derby" );
  private final static String CWD = System.getProperty( "user.dir" );
  private final static String HIVE_WAREHOUSE_DIR = CWD + "/build/test/hive";

  public HiveFlowTest()
    {
    super( false );
    }

  @Before
  public void setUp() throws IOException
    {
    // the hive meta store uses derby by default
    if( DERBY_HOME.exists() )
      FileUtils.deleteDirectory( DERBY_HOME );

    System.setProperty( "derby.system.home", DERBY_HOME.getAbsolutePath() );
    }


  @Test(expected = CascadingException.class)
  public void testWrongSQL()
    {
    new HiveFlow( "broken", new Properties(), "create tble foobar", createHiveConf() );
    }


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

    }

  @Test
  public void testJoin() throws IOException
    {
    CascadeConnector connector = new CascadeConnector();

    HiveFlow createFirstTableFlow = new HiveFlow( "create keyvalue table", new Properties(),
      " create table keyvalue (key int, value string) row format delimited fields terminated by '\t' ",
      createHiveConf() );

    HiveFlow createSecondTableFlow = new HiveFlow( "create keyvalue2 table", new Properties(),
      " create table keyvalue2 (key int, value string) row format delimited fields terminated by '\t' ",
      createHiveConf() );
    connector.connect( createFirstTableFlow, createSecondTableFlow ).complete();

    HiveFlow multitableSelect = new HiveFlow( "select from multiple tables", new Properties(),
      "select keyvalue.key, keyvalue.value from keyvalue join keyvalue2 on keyvalue.key = keyvalue2.key",
      createHiveConf() );

    Map<String, Tap> sources = multitableSelect.getSources();
    assertEquals( 2, sources.size() );
    Tap firstTable = sources.get( "file://" + HIVE_WAREHOUSE_DIR + "keyvalue" );
    assertNotNull( firstTable );
    Tap secondTable = sources.get( "file://" + HIVE_WAREHOUSE_DIR + "keyvalue2" );
    assertNotNull( secondTable );

    }

  private HiveConf createHiveConf()
    {
    HiveConf conf = new HiveConf();
    conf.set( HiveConf.ConfVars.METASTOREWAREHOUSE.varname, HIVE_WAREHOUSE_DIR );
    return conf;
    }

  }
