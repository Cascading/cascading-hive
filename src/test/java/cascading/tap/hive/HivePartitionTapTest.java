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

package cascading.tap.hive;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;

import cascading.flow.FlowProcess;
import cascading.flow.hadoop.HadoopFlowProcess;
import cascading.scheme.NullScheme;
import cascading.scheme.Scheme;
import cascading.tap.SinkMode;
import cascading.tap.hadoop.io.MultiInputSplit;
import cascading.tuple.TupleEntryCollector;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.thrift.TException;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;

import cascading.CascadingException;

import static org.junit.Assert.*;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests for HivePartitionTap.
 */
@RunWith(MockitoJUnitRunner.class)
public class HivePartitionTapTest
  {

  private static final String DATABASE_NAME = "test_db";
  private static final String TABLE_NAME = "dual";
  private static final String[] COLUMN_NAMES = new String[]{"key", "val"};
  private static final String[] COLUMN_TYPES = new String[]{"int", "string"};
  private static final String[] PARTITION_KEYS = new String[]{"key"};

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Mock
  private FlowProcess mockFlowProcess;

  private HiveTableDescriptor desc = new HiveTableDescriptor( TABLE_NAME, COLUMN_NAMES,
    COLUMN_TYPES,
    PARTITION_KEYS );
  private HiveTap parent = new HiveTap( desc, new NullScheme() );

  @Test
  public void testConstruction()
    {
    HivePartitionTap partitionTap = new HivePartitionTap( parent, SinkMode.UPDATE );
    assertSame( partitionTap.getParent(), parent );
    assertEquals( SinkMode.UPDATE, partitionTap.getSinkMode() );
    }

  @Test
  public void openForWrite() throws IOException
    {
    HivePartitionTap partitionTap = new HivePartitionTap( parent );

    FlowProcess process = new HadoopFlowProcess();
    OutputCollector collector = Mockito.mock( OutputCollector.class );
    TupleEntryCollector tec = partitionTap.openForWrite( process, collector );
    HivePartitionTap.HivePartitionCollector partitionCollector = (HivePartitionTap.HivePartitionCollector) tec;
    assertNotNull( partitionCollector );
    }

  @Test
  public void getCurrentIdentifierNull() throws IOException
    {
    HivePartitionTap partitionTap = new HivePartitionTap( parent );

    when( mockFlowProcess.getStringProperty( MultiInputSplit.CASCADING_SOURCE_PATH ) ).thenReturn( null );
    when( mockFlowProcess.getConfigCopy() ).thenReturn( HiveConfFactory.getHiveConf( null ) );

    String identifier = partitionTap.getCurrentIdentifier( mockFlowProcess );
    assertNull( identifier );
    }

  @Test
  public void getCurrentIdentifierPart() throws IOException
    {
    HivePartitionTap partitionTap = new HivePartitionTap( parent );

    File folder = temporaryFolder.newFolder( "A", "B" );
    File part = new File( folder, "part-00000" );
    part.createNewFile();

    when( mockFlowProcess.getStringProperty( MultiInputSplit.CASCADING_SOURCE_PATH ) ).thenReturn( part.toURI().toString() );
    when( mockFlowProcess.getConfigCopy() ).thenReturn( HiveConfFactory.getHiveConf( null ) );

    String identifier = partitionTap.getCurrentIdentifier( mockFlowProcess );
    String folderUri = folder.toURI().toString();
    folderUri = folderUri.substring( 0, folderUri.length() - 1 );
    assertEquals( folderUri, identifier );
    }

  @Test
  public void getCurrentIdentifierOrcAcidBase() throws IOException
    {
    HiveTableDescriptor acidDesc = new HiveTableDescriptor( TABLE_NAME, COLUMN_NAMES, COLUMN_TYPES, PARTITION_KEYS,
      true );
    HiveTap acidParent = new HiveTap( acidDesc, new NullScheme() );
    HivePartitionTap partitionTap = new HivePartitionTap( acidParent );

    File deltaFolder = temporaryFolder.newFolder( "A", "B", "delta_0000001_0000001" );
    File deltaFile = new File( deltaFolder, "bucket_00001" );
    deltaFile.createNewFile();

    File baseFolder = temporaryFolder.newFolder( "A", "B", "base_0000001_0000001" );
    File baseFile = new File( baseFolder, "bucket_00001" );
    baseFile.createNewFile();

    when( mockFlowProcess.getStringProperty( MultiInputSplit.CASCADING_SOURCE_PATH ) )
      .thenReturn( baseFolder.toURI().toString() );
    when( mockFlowProcess.getConfigCopy() ).thenReturn( HiveConfFactory.getHiveConf( null ) );

    String identifier = partitionTap.getCurrentIdentifier( mockFlowProcess );
    String folderUri = baseFolder.getParentFile().toURI().toString();
    folderUri = folderUri.substring( 0, folderUri.length() - 1 );
    assertEquals( folderUri, identifier );
    }

  @Test
  public void getCurrentIdentifierOrcAcidDelta() throws IOException
    {
    HiveTableDescriptor acidDesc = new HiveTableDescriptor( TABLE_NAME, COLUMN_NAMES, COLUMN_TYPES, PARTITION_KEYS,
      true );
    HiveTap acidParent = new HiveTap( acidDesc, new NullScheme() );
    HivePartitionTap partitionTap = new HivePartitionTap( acidParent );

    File deltaFolder = temporaryFolder.newFolder( "A", "B", "delta_0000001_0000001" );
    File deltaFile = new File( deltaFolder, "bucket_00001" );
    deltaFile.createNewFile();

    when( mockFlowProcess.getConfigCopy() ).thenReturn( HiveConfFactory.getHiveConf( null ) );
    when( mockFlowProcess.getStringProperty( MultiInputSplit.CASCADING_SOURCE_PATH ) )
      .thenReturn( deltaFolder.getParentFile().toURI().toString() );

    String identifier = partitionTap.getCurrentIdentifier( mockFlowProcess );
    String folderUri = deltaFolder.getParentFile().toURI().toString();
    folderUri = folderUri.substring( 0, folderUri.length() - 1 );
    assertEquals( folderUri, identifier );
    }

  @Test
  public void getChildPartitionIdentifiersReadAll() throws IOException, TException
    {
    HiveTableDescriptor tableDesc = new HiveTableDescriptor( DATABASE_NAME, TABLE_NAME, COLUMN_NAMES, COLUMN_TYPES,
        PARTITION_KEYS );

    MetaStoreClientFactory metaStoreClientFactory = mock( MetaStoreClientFactory.class );
    IMetaStoreClient metaStoreClient = mock( IMetaStoreClient.class );
    Table table = mock( Table.class );
    StorageDescriptor tableStorageDescriptor = mock( StorageDescriptor.class );
    Partition partition = mock( Partition.class );
    StorageDescriptor partitionStorageDescriptor = mock( StorageDescriptor.class );

    when( mockFlowProcess.getConfig() ).thenReturn( HiveConfFactory.getHiveConf( null ) );
    when( metaStoreClientFactory.newInstance( any( HiveConf.class) ) ).thenReturn( metaStoreClient );
    when( metaStoreClient.getTable( DATABASE_NAME, TABLE_NAME ) ).thenReturn( table );
    when( table.getSd() ).thenReturn( tableStorageDescriptor );
    when( tableStorageDescriptor.getLocation() ).thenReturn( "/base/path" );
    when( metaStoreClient.listPartitions( DATABASE_NAME, TABLE_NAME, HivePartitionTap.NO_LIMIT ) )
        .thenReturn( Arrays.asList( partition ) );
    when( partition.getSd() ).thenReturn( partitionStorageDescriptor );
    when( partitionStorageDescriptor.getLocation() ).thenReturn( "/base/path/key=0" );

    Scheme scheme = new NullScheme();
    SinkMode sinkMode = SinkMode.KEEP;
    HiveTap hiveTap = new HiveTap( tableDesc, scheme, sinkMode, false, metaStoreClientFactory );
    HivePartitionTap partitionTap = new HivePartitionTap( hiveTap );

    String[] identifiers = partitionTap.getChildPartitionIdentifiers( mockFlowProcess, true );
    assertEquals( 1, identifiers.length );
    assertEquals( "/base/path/key=0", identifiers[0] );
    }

  @Test
  public void getChildPartitionIdentifiersReadSelected() throws IOException, TException
    {
    HiveTableDescriptor tableDesc = new HiveTableDescriptor( DATABASE_NAME, TABLE_NAME, COLUMN_NAMES, COLUMN_TYPES,
        PARTITION_KEYS );

    MetaStoreClientFactory metaStoreClientFactory = mock( MetaStoreClientFactory.class );
    IMetaStoreClient metaStoreClient = mock( IMetaStoreClient.class );
    Table table = mock( Table.class );
    StorageDescriptor tableStorageDescriptor = mock( StorageDescriptor.class );
    Partition partition = mock( Partition.class );
    StorageDescriptor partitionStorageDescriptor = mock( StorageDescriptor.class );

    when( mockFlowProcess.getConfig() ).thenReturn( HiveConfFactory.getHiveConf( null ) );
    when( metaStoreClientFactory.newInstance( any( HiveConf.class) ) ).thenReturn( metaStoreClient );
    when( metaStoreClient.getTable( DATABASE_NAME, TABLE_NAME ) ).thenReturn( table );
    when( table.getSd() ).thenReturn( tableStorageDescriptor );
    when( tableStorageDescriptor.getLocation() ).thenReturn( "/base/path" );
    when( metaStoreClient.getPartitionsByNames( DATABASE_NAME, TABLE_NAME, Arrays.asList( "key=0" ) ) )
        .thenReturn( Arrays.asList( partition ) );
    when( partition.getSd() ).thenReturn( partitionStorageDescriptor );
    when( partitionStorageDescriptor.getLocation() ).thenReturn( "/base/path/key=0" );
    when(partition.getValues()).thenReturn(Arrays.asList("0"));

    Scheme scheme = new NullScheme();
    SinkMode sinkMode = SinkMode.KEEP;
    HiveTap hiveTap = new HiveTap( tableDesc, scheme, sinkMode, false, metaStoreClientFactory );
    HivePartitionTap partitionTap = new HivePartitionTap( hiveTap, Arrays.asList( Arrays.asList( "0" ) ) );

    String[] identifiers = partitionTap.getChildPartitionIdentifiers( mockFlowProcess, true );
    assertEquals( 1, identifiers.length );
    assertEquals( "/base/path/key=0", identifiers[0] );
    }

  @Test( expected = CascadingException.class )
  public void getChildPartitionIdentifiersReadSelectedNotExistsFail() throws IOException, TException
    {
    HiveTableDescriptor tableDesc = new HiveTableDescriptor( DATABASE_NAME, TABLE_NAME, COLUMN_NAMES, COLUMN_TYPES,
        PARTITION_KEYS );

    MetaStoreClientFactory metaStoreClientFactory = mock( MetaStoreClientFactory.class );
    IMetaStoreClient metaStoreClient = mock( IMetaStoreClient.class );
    Table table = mock( Table.class );
    StorageDescriptor tableStorageDescriptor = mock( StorageDescriptor.class );

    when( mockFlowProcess.getConfig() ).thenReturn( HiveConfFactory.getHiveConf( null ) );
    when( metaStoreClientFactory.newInstance( any( HiveConf.class) ) ).thenReturn( metaStoreClient );
    when( metaStoreClient.getTable( DATABASE_NAME, TABLE_NAME ) ).thenReturn( table );
    when( table.getSd() ).thenReturn( tableStorageDescriptor );
    when( tableStorageDescriptor.getLocation() ).thenReturn( "/base/path" );
    when( metaStoreClient.getPartitionsByNames( DATABASE_NAME, TABLE_NAME, Arrays.asList( "key=0" ) ) )
        .thenReturn( new ArrayList<Partition>() );

    Scheme scheme = new NullScheme();
    SinkMode sinkMode = SinkMode.KEEP;
    HiveTap hiveTap = new HiveTap( tableDesc, scheme, sinkMode, false, metaStoreClientFactory );
    HivePartitionTap partitionTap = new HivePartitionTap( hiveTap, Arrays.asList( Arrays.asList( "0" ) ) );

    partitionTap.getChildPartitionIdentifiers( mockFlowProcess, true );
    }

  @Test
  public void getChildPartitionIdentifiersReadSelectedNotExistsOverride() throws IOException, TException
    {
    HiveTableDescriptor tableDesc = new HiveTableDescriptor( DATABASE_NAME, TABLE_NAME, COLUMN_NAMES, COLUMN_TYPES,
        PARTITION_KEYS );

    MetaStoreClientFactory metaStoreClientFactory = mock( MetaStoreClientFactory.class );
    IMetaStoreClient metaStoreClient = mock( IMetaStoreClient.class );
    Table table = mock( Table.class );
    StorageDescriptor tableStorageDescriptor = mock( StorageDescriptor.class );

    HiveConf hiveConf = HiveConfFactory.getHiveConf( null );
    hiveConf.setBoolean(HivePartitionTap.FAIL_ON_MISSING_PARTITION, false);
    when( mockFlowProcess.getConfig() ).thenReturn( hiveConf );
    when( metaStoreClientFactory.newInstance( any( HiveConf.class) ) ).thenReturn( metaStoreClient );
    when( metaStoreClient.getTable( DATABASE_NAME, TABLE_NAME ) ).thenReturn( table );
    when( table.getSd() ).thenReturn( tableStorageDescriptor );
    when( tableStorageDescriptor.getLocation() ).thenReturn( "/base/path" );
    when( metaStoreClient.getPartitionsByNames( DATABASE_NAME, TABLE_NAME, Arrays.asList( "key=0" ) ) )
        .thenReturn( new ArrayList<Partition>() );

    Scheme scheme = new NullScheme();
    SinkMode sinkMode = SinkMode.KEEP;
    HiveTap hiveTap = new HiveTap( tableDesc, scheme, sinkMode, false, metaStoreClientFactory );
    HivePartitionTap partitionTap = new HivePartitionTap( hiveTap, Arrays.asList( Arrays.asList( "0" ) ) );

    String[] identifiers = partitionTap.getChildPartitionIdentifiers( mockFlowProcess, true );
    assertEquals( 0, identifiers.length );
    }

  }
