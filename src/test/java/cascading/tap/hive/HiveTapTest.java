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

package cascading.tap.hive;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;

import cascading.HiveTestCase;
import cascading.scheme.NullScheme;
import cascading.tap.SinkMode;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.ValidTxnList;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.txn.TxnDbUtil;
import org.apache.hadoop.mapred.JobConf;
import org.junit.Test;

/**
 * Tests for HiveTap.
 */
public class HiveTapTest extends HiveTestCase
  {
  @Test
  public void testResourceExistsWithNonExistingTable() throws IOException
    {
    HiveTableDescriptor desc = new HiveTableDescriptor( "myTable", new String[]{"key"}, new String[]{"string"} );
    HiveTap tap = new HiveTap( desc, new NullScheme() );
    assertFalse( tap.resourceExists( new JobConf() ) );
    }

  @Test
  public void testCreateResource() throws IOException
    {
    HiveTableDescriptor desc = new HiveTableDescriptor( "myTable2", new String[]{"key"}, new String[]{"string"} );
    HiveTap tap = new HiveTap( desc, new NullScheme() );
    assertTrue( tap.createResource( new JobConf() ) );
    assertTrue( tap.resourceExists( new JobConf() ) );
    assertNotNull( tap.getPath() );
    }

  @Test
  public void testCreateResourceInNonExistingDatabase() throws IOException
    {
    HiveTableDescriptor desc = new HiveTableDescriptor( "myDatabase", "myTable2", new String[]{"key"}, new String[]{
      "string"} );
    HiveTap tap = new HiveTap( desc, new NullScheme() );
    assertTrue( tap.createResource( new JobConf() ) );
    assertTrue( tap.resourceExists( new JobConf() ) );
    assertNotNull( tap.getPath() );
    }

  @Test(expected = HiveTableValidationException.class)
  public void testResourceExistsStrictModeColumnCountMismatch() throws IOException
    {
    HiveTableDescriptor desc = new HiveTableDescriptor( "myTable3", new String[]{"key"}, new String[]{"string"} );
    HiveTap tap = new HiveTap( desc, new NullScheme() );
    tap.createResource( new JobConf() );

    HiveTableDescriptor mismatch = new HiveTableDescriptor( "myTable3", new String[]{"key", "value"},
      new String[]{"string", "string"} );

    tap = new HiveTap( mismatch, new NullScheme(), SinkMode.REPLACE, true );
    tap.resourceExists( new JobConf() );

    }

  @Test(expected = HiveTableValidationException.class)
  public void testResourceExistsStrictModeNameMismatch() throws IOException
    {
    HiveTableDescriptor desc = new HiveTableDescriptor( "myTable4", new String[]{"key"}, new String[]{"string"} );
    HiveTap tap = new HiveTap( desc, new NullScheme() );
    tap.createResource( new JobConf() );

    HiveTableDescriptor mismatch = new HiveTableDescriptor( "myTable4", new String[]{"key2"}, new String[]{"string"} );

    tap = new HiveTap( mismatch, new NullScheme(), SinkMode.REPLACE, true );
    tap.resourceExists( new JobConf() );
    }

  @Test(expected = HiveTableValidationException.class)
  public void testResourceExistsStrictModeTypeMismatch() throws IOException
    {
    HiveTableDescriptor desc = new HiveTableDescriptor( "myTable5", new String[]{"key"}, new String[]{"string"} );
    HiveTap tap = new HiveTap( desc, new NullScheme() );
    tap.createResource( new JobConf() );

    HiveTableDescriptor mismatch = new HiveTableDescriptor( "myTable5", new String[]{"key"}, new String[]{"int"} );
    tap = new HiveTap( mismatch, new NullScheme(), SinkMode.REPLACE, true );
    tap.resourceExists( new JobConf() );
    }

  @Test
  public void testResourceExistsStrictModeCaseInsensitivity() throws IOException
    {
    HiveTableDescriptor desc = new HiveTableDescriptor( "myTable4", new String[]{"key"}, new String[]{"string"} );
    HiveTap tap = new HiveTap( desc, new NullScheme() );
    tap.createResource( new JobConf() );

    HiveTableDescriptor mismatch = new HiveTableDescriptor( "MYTABLE4", new String[]{"KeY"}, new String[]{"StRinG"} );

    tap = new HiveTap( mismatch, new NullScheme(), SinkMode.REPLACE, true );
    assertTrue( tap.resourceExists( new JobConf() ) );
    }

  @Test
  public void testResourceExistsStrictModeWithPartitionedTable() throws IOException
    {
    HiveTableDescriptor desc = new HiveTableDescriptor( "myTable8", new String[]{"name", "id"},
      new String[]{"string", "string"},
      new String[]{"id"} );
    HiveTap tap = new HiveTap( desc, new NullScheme() );
    tap.createResource( new JobConf() );

    HiveTableDescriptor mismatch = new HiveTableDescriptor( "MYTABLE8", new String[]{"NAME", "ID"},
      new String[]{"StRinG", "string"},
      new String[]{"ID"} );

    tap = new HiveTap( mismatch, new NullScheme(), SinkMode.REPLACE, true );
    assertTrue( tap.resourceExists( new JobConf() ) );
    }

  @Test(expected = HiveTableValidationException.class)
  public void testResourceExistsStrictModeLocationMismatch() throws IOException
    {
    HiveTableDescriptor desc = new HiveTableDescriptor( HiveTableDescriptor.HIVE_DEFAULT_DATABASE_NAME, "mytable9",
      new String[]{"one", "two", "three"},
      new String[]{"int", "string", "boolean"}, new String[]{},
      ",", HiveTableDescriptor.HIVE_DEFAULT_SERIALIZATION_LIB_NAME, new Path( dbFolder.getRoot().getAbsolutePath() + "/custompath" ) );
    HiveTap tap = new HiveTap( desc, new NullScheme() );
    tap.createResource( new JobConf() );

    HiveTableDescriptor mismatch = new HiveTableDescriptor( HiveTableDescriptor.HIVE_DEFAULT_DATABASE_NAME, "mytable9",
      new String[]{"one", "two", "three"},
      new String[]{"int", "string", "boolean"}, new String[]{},
      "," );
    tap = new HiveTap( mismatch, new NullScheme(), SinkMode.REPLACE, true );
    tap.resourceExists( new JobConf() );
    }

  @Test
  public void testDeleteRessource() throws Exception
    {
    HiveTableDescriptor desc = new HiveTableDescriptor( "myTable5", new String[]{"key"}, new String[]{"string"} );
    HiveTap tap = new HiveTap( desc, new NullScheme() );

    JobConf conf = new JobConf();

    tap.createResource( conf );
    assertTrue( tap.resourceExists( conf ) );
    assertTableExists( desc );

    tap.deleteResource( conf );
    assertFalse( tap.resourceExists( conf ) );
    }

  @Test
  public void testRegisterPartition() throws Exception
    {
    HiveTableDescriptor desc = new HiveTableDescriptor( "myTable6", new String[]{"one", "two"},
      new String[]{"string", "string"}, new String[]{"two"} );
    HiveTap tap = new HiveTap( desc, new NullScheme() );
    JobConf conf = new JobConf();
    int now = (int) ( System.currentTimeMillis() / 1000 );
    Partition part = new Partition( Arrays.asList( "2" ), desc.getDatabaseName(),
      desc.getTableName(), now, now, desc.toHiveTable().getSd(),
      new HashMap<String, String>() );

    tap.registerPartition( conf, part );

    assertTableExists( desc );

    IMetaStoreClient client = createMetaStoreClient();
    Partition result = client.getPartition( desc.getDatabaseName(), desc.getTableName(), Arrays.asList( "2" ) );
    assertNotNull( result );
    client.close();
    }

  @Test
  public void testGetPathWithExistingTableInDifferentLocation()
    {
    runHiveQuery( "create table myTable7 (one string, two string) location '/tmp/myLocation'" );
    HiveTableDescriptor desc = new HiveTableDescriptor( "myTable7", new String[]{"one", "two"},
      new String[]{"string", "string"}, new String[]{"two"} );
    HiveTap tap = new HiveTap( desc, new NullScheme() );
    assertEquals( "file:/tmp/myLocation", tap.getPath().toString() );
    }

  @Test
  public void testSetTransactionalConfig() throws Exception
    {
    HiveConf conf = new HiveConf();
    TxnDbUtil.setConfValues( conf );
    conf.setBoolVar( HiveConf.ConfVars.METASTORE_EXECUTE_SET_UGI, true );
    conf.setBoolVar( HiveConf.ConfVars.HIVE_SUPPORT_CONCURRENCY, true );
    conf.unset( ValidTxnList.VALID_TXNS_KEY );
    TxnDbUtil.cleanDb();
    TxnDbUtil.prepDb();

    HiveTableDescriptor desc = new HiveTableDescriptor( "myTableTx1", new String[]{"key"}, new String[]{
      "string"}, true );
    HiveTap tap = new HiveTap( desc, new NullScheme() );
    tap.sourceConfInit( null, conf );

    assertEquals( "0:", conf.get( ValidTxnList.VALID_TXNS_KEY ) );
    }

  @Test(expected = UnsupportedOperationException.class)
  public void testSinkConfInitFailsForTransactional()
    {
    HiveTableDescriptor desc = new HiveTableDescriptor( "myTableTx2", new String[]{"key"}, new String[]{
      "string"}, true );
    HiveTap tap = new HiveTap( desc, new NullScheme() );
    tap.sinkConfInit( null, new JobConf() );
    }

  @Test(expected = UnsupportedOperationException.class)
  public void testOpenForWriteFailsForTransactional() throws IOException
    {
    HiveTableDescriptor desc = new HiveTableDescriptor( "myTableTx3", new String[]{"key"}, new String[]{
      "string"}, true );
    HiveTap tap = new HiveTap( desc, new NullScheme() );
    tap.openForWrite( null, null );
    }

  @Test(expected = UnsupportedOperationException.class)
  public void testCreateResourceFailsForTransactional() throws IOException
    {
    HiveTableDescriptor desc = new HiveTableDescriptor( "myTableTx4", new String[]{"key"}, new String[]{
      "string"}, true );
    HiveTap tap = new HiveTap( desc, new NullScheme() );
    tap.createResource( new JobConf() );
    }

  @Test(expected = UnsupportedOperationException.class)
  public void testDeleteResourceFailsForTransactional() throws IOException
    {
    HiveTableDescriptor desc = new HiveTableDescriptor( "myTableTx5", new String[]{"key"}, new String[]{
      "string"}, true );
    HiveTap tap = new HiveTap( desc, new NullScheme() );
    tap.deleteResource( new JobConf() );
    }

  private void assertTableExists( HiveTableDescriptor descriptor ) throws Exception
    {
    IMetaStoreClient client = createMetaStoreClient();
    assertTrue( client.tableExists( descriptor.getDatabaseName(), descriptor.getTableName() ) );
    client.close();
    }

  }
