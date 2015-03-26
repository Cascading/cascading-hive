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

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import cascading.HiveTestCase;
import cascading.flow.FlowDescriptors;
import cascading.scheme.NullScheme;
import cascading.tap.Tap;
import cascading.tap.hadoop.Hfs;
import org.junit.Test;

/**
 * Test for HiveFlow.
 */
public class HiveFlowTest extends HiveTestCase
  {
  @Test
  public void testConstruction()
    {
    Tap sink = new Hfs( new NullScheme(), "/foo" );
    List<Tap> sources = Arrays.asList( (Tap) new Hfs( new NullScheme(), "/bar" ) );

    String stmt = "select * from foo";
    HiveFlow flow = new HiveFlow( "some name", createHiveDriverFactory(), stmt, sources, sink );
    assertEquals( sink, flow.getSink() );
    Map<String, Tap> expectedSources = new HashMap<String, Tap>();
    expectedSources.put( "/bar", sources.get( 0 ) );
    assertEquals( expectedSources, flow.getSources() );
    assertEquals( "some name", flow.getName() );

    Map<String, String> flowDescriptor = flow.getFlowDescriptor();
    assertEquals( 2, flowDescriptor.size() );
    assertEquals( "Hive flow", flowDescriptor.get( FlowDescriptors.DESCRIPTION ) );
    assertEquals( stmt, flowDescriptor.get( FlowDescriptors.STATEMENTS ) );
    }

  @Test
  public void testConstructionMultiple()
    {
    String queries[] = {
      "select * from foo",
      "select count(*) from foo"
    };

    Tap sink = new Hfs( new NullScheme(), "/foo" );
    List<Tap> sources = Arrays.asList( (Tap) new Hfs( new NullScheme(), "/bar" ) );
    HiveFlow flow = new HiveFlow( "some name", createHiveDriverFactory(),
      queries, sources, sink );
    assertEquals( sink, flow.getSink() );
    Map<String, Tap> expectedSources = new HashMap<String, Tap>();
    expectedSources.put( "/bar", sources.get( 0 ) );
    assertEquals( expectedSources, flow.getSources() );
    assertEquals( "some name", flow.getName() );

    Map<String, String> flowDescriptor = flow.getFlowDescriptor();
    assertEquals( 2, flowDescriptor.size() );
    assertEquals( "Hive flow", flowDescriptor.get( FlowDescriptors.DESCRIPTION ) );
    assertEquals( queries[ 0 ] + FlowDescriptors.VALUE_SEPARATOR + queries[ 1 ], flowDescriptor.get( FlowDescriptors.STATEMENTS ) );
    }
  }
