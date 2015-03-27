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

import java.io.IOException;

import cascading.CascadingException;
import cascading.tap.SinkMode;
import cascading.flow.FlowProcess;
import cascading.tap.hadoop.PartitionTap;
import cascading.tuple.TupleEntryCollector;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.OutputCollector;

/**
 * Subclass of PartitionTap which registers partitions created in a Cascading Flow in the HiveMetaStore. Since the registering
 * is happening cluster side, the MetaStore has to be deployed as a standalone service.
 */
public class HivePartitionTap extends PartitionTap
  {
  /**
   * Constructs a new HivePartitionTap with the given HiveTap as the parent directory.
   * @param parent The parent directory.
   */
  public HivePartitionTap( HiveTap parent )
    {
    super( parent, parent.getTableDescriptor().getPartition() );
    }

  /**
   * Constructs a new HivePartitionTap with the given HiveTap as the parent directory and the given SinkMode.
   * @param parent The parent directory.
   * @param sinkMode The sinkMode of this tap.
   */
  public HivePartitionTap( HiveTap parent, SinkMode sinkMode )
    {
    super( parent, parent.getTableDescriptor().getPartition(), sinkMode );
    }

  /**
   * Subclass of PartitionCollector, which will register each partition in the HiveMetaStore on the fly.
   */
  class HivePartitionCollector extends PartitionCollector
    {
    private FlowProcess<? extends Configuration> flowProcess;

    /**
     * Constructs a new HivePartitionCollector instance with the current FlowProcess instance.
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

  @Override
  public TupleEntryCollector openForWrite( FlowProcess<? extends Configuration> flowProcess, OutputCollector output ) throws IOException
    {
    return new HivePartitionCollector( flowProcess );
    }

  @Override
  public String getFullIdentifier( Configuration conf )
    {
    return parent.getFullIdentifier( conf );
    }

  }
