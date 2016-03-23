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

import java.io.File;
import java.io.IOException;

import cascading.flow.FlowProcess;
import cascading.flow.hadoop.HadoopFlowProcess;
import cascading.scheme.NullScheme;
import cascading.tap.SinkMode;
import cascading.tap.hadoop.io.MultiInputSplit;
import cascading.tuple.TupleEntryCollector;
import org.apache.hadoop.mapred.OutputCollector;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;

import static org.junit.Assert.*;
import static org.mockito.Mockito.when;

/**
 * Tests for HivePartitionTap.
 */
@RunWith(MockitoJUnitRunner.class)
public class HivePartitionTapTest
  {

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

  }
