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

import java.util.Collections;
import java.util.Map;

import org.junit.Test;
import riffle.process.scheduler.ProcessWrapper;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Test for HiveStep.
 */
public class HiveStepTest
  {

  @Test
  public void testConstruction() throws Exception
    {
    HiveStep step = new HiveStep( "id", Collections.<String, Map<String, Long>>emptyMap() );
    ProcessWrapper processWrapper = new ProcessWrapper( step );
    assertTrue( processWrapper.hasCounters() );
    assertFalse( processWrapper.hasChildren() );
    }
  }
