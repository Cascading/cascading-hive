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

package cascading.tap.hive;

import java.io.IOException;

import cascading.CascadingException;
import cascading.flow.FlowProcess;
import cascading.tap.hadoop.PartitionTap;
import cascading.tuple.TupleEntry;
import cascading.tuple.TupleEntryCollector;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;

/**
 * Subclass of PartitionTap which registers partitions created in a Cascading Flow in the HiveMetaStore. Since the registering
 * is happening cluster side, the MetaStore has to be a remote.
 */
public class HivePartitionTap extends PartitionTap
  {
  public HivePartitionTap( HiveTap parent )
    {
    super( parent, parent.getTableDescriptor().getPartition() );
    }

  class HivePartitionCollector extends PartitionCollector
    {
    private final FlowProcess<JobConf> flowProcess;

    public HivePartitionCollector( FlowProcess<JobConf> flowProcess )
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
  public TupleEntryCollector openForWrite( FlowProcess<JobConf> flowProcess, OutputCollector output ) throws IOException
    {
    return new HivePartitionCollector( flowProcess );
    }

  }
