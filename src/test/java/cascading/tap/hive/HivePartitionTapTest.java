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
import cascading.flow.hadoop.HadoopFlowProcess;
import cascading.scheme.NullScheme;
import cascading.tuple.TupleEntryCollector;
import org.apache.hadoop.mapred.OutputCollector;
import org.junit.Test;
import org.mockito.Mockito;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;

/**
 * Tests for HivePartitionTap.
 */
public class HivePartitionTapTest
  {

  @Test
  public void testConstruction()
    {
    HiveTableDescriptor desc = new HiveTableDescriptor( "dual", new String[]{"key", "val"},
                                                                new String[]{"int", "string"},
                                                                new String[]{"key"} );
    HiveTap tap = new HiveTap( desc, new NullScheme(  ) );
    HivePartitionTap partitionTap =  new HivePartitionTap( tap );
    assertSame( partitionTap.getParent(), tap );
    }

  @Test
  public void openForWrite() throws IOException
    {
    HiveTableDescriptor desc = new HiveTableDescriptor( "dual", new String[]{"key", "val"},
      new String[]{"int", "string"},
      new String[]{"key"} );
    HiveTap parent = new HiveTap( desc, new NullScheme(  ) );
    HivePartitionTap partitionTap =  new HivePartitionTap( parent );

    FlowProcess process = new HadoopFlowProcess(  );
    OutputCollector collector = Mockito.mock( OutputCollector.class );
    TupleEntryCollector tec =  partitionTap.openForWrite( process, collector );
    HivePartitionTap.HivePartitionCollector partitionCollector = (HivePartitionTap.HivePartitionCollector) tec;
    assertNotNull( partitionCollector );
    }


  }
