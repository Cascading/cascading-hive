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

import cascading.CascadingException;
import cascading.flow.FlowProcess;
import cascading.tap.SinkMode;
import cascading.tap.TapException;
import cascading.tap.hadoop.PartitionTap;
import cascading.tap.hadoop.io.MultiInputSplit;
import cascading.tuple.TupleEntryCollector;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.AcidUtils;
import org.apache.hadoop.mapred.OutputCollector;

/**
 * Subclass of PartitionTap which registers partitions created in a Cascading Flow in the HiveMetaStore. Since the
 * registering is happening cluster side, the MetaStore has to be deployed as a standalone service.
 */
public class HivePartitionTap extends PartitionTap
  {
  /**
   * Constructs a new HivePartitionTap with the given HiveTap as the parent directory.
   *
   * @param parent The parent directory.
   */
  public HivePartitionTap( HiveTap parent )
    {
    super( parent, parent.getTableDescriptor().getPartition() );
    }

  /**
   * Constructs a new HivePartitionTap with the given HiveTap as the parent directory and the given SinkMode.
   *
   * @param parent   The parent directory.
   * @param sinkMode The sinkMode of this tap.
   */
  public HivePartitionTap( HiveTap parent, SinkMode sinkMode )
    {
    super( parent, parent.getTableDescriptor().getPartition(), sinkMode );
    }

  @Override
  public TupleEntryCollector openForWrite( FlowProcess<? extends Configuration> flowProcess, OutputCollector output )
    throws IOException
    {
    return new HivePartitionCollector( flowProcess );
    }

  @Override
  public String getFullIdentifier( Configuration conf )
    {
    return parent.getFullIdentifier( conf );
    }

  /**
   * This is horrible but needed. Cascading expects to strip off a 'part-00000' but in the case of transactional tables
   * that have only delta files, there is no part on the path. Without this we lose our last partition path element in
   * this case.
   * <p/>
   * See <a href="http://mail-archives.apache.org/mod_mbox/hive-user/201605.mbox/%3CCAC3gpCYxb4mnnFDTKXFP2XCT-chERx64iaAO6VpsnmU+SYoGuQ@mail.gmail.com%3E">this thread</a>.
   */
  @Override
  protected String getCurrentIdentifier( FlowProcess<? extends Configuration> flowProcess )
    {
    // set on current split
    String identifier = flowProcess.getStringProperty( MultiInputSplit.CASCADING_SOURCE_PATH );

    if( identifier == null )
      return null;

    try
      {
      if( ( (HiveTap) parent ).isTransactional() )
        {
        FileSystem fs = FileSystem.get( flowProcess.getConfigCopy() );
        Path path = new Path( identifier );
        FileStatus[] baseFolders = fs.listStatus( path, AcidUtils.baseFileFilter );
        if( baseFolders.length == 0 )
          {
          FileStatus[] deltaFolders = fs.listStatus( path, AcidUtils.deltaFileFilter );

          if( deltaFolders.length > 0 )
            return new Path( identifier ).toString();

          }
        }
      }
    catch( IOException e )
      {
      throw new TapException( e );
      }
    return new Path( identifier ).getParent().toString(); // drop part-xxxx
    }

  @Override
  public void sourceConfInit( FlowProcess<? extends Configuration> flowProcess, Configuration conf )
    {
    ( (HiveTap) getParent() ).setTransactionalConfig( conf );
    super.sourceConfInit( flowProcess, conf );
    }

  /**
   * Subclass of PartitionCollector, which will register each partition in the HiveMetaStore on the fly.
   */
  class HivePartitionCollector extends PartitionCollector
    {
    private FlowProcess<? extends Configuration> flowProcess;

    /**
     * Constructs a new HivePartitionCollector instance with the current FlowProcess instance.
     *
     * @param flowProcess The currently running FlowProcess.
     */
    public HivePartitionCollector( FlowProcess<? extends Configuration> flowProcess )
      {
      super( flowProcess );
      this.flowProcess = flowProcess;
      }

    @Override
    public void closeCollector( String path )
      {
      HivePartition partition = (HivePartition) getPartition();
      HiveTap tap = (HiveTap) getParent();
      try
        {
        // register the new partition, when we close the collector. If it already exists, nothing will happen.
        tap.registerPartition( flowProcess.getConfigCopy(), partition.toHivePartition( path, tap.getTableDescriptor() ) );
        }
      catch( IOException exception )
        {
        throw new CascadingException( exception );
        }
      finally
        {
        super.closeCollector( path );
        }
      }
    }

  }
