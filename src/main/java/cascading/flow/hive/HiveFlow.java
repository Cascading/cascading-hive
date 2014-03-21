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

import java.util.List;

import cascading.flow.hadoop.ProcessFlow;
import cascading.tap.Tap;

/**
 * A subclass of ProcessFlow for running Hive queries.
 */
public class HiveFlow extends ProcessFlow
  {

  /**
   * Constructs a new HiveFlow object with the given name, query, a list of source taps and sink.
   *
   * @param name The name of the flow.
   * @param query   The hive query to run.
   * @param sources The source taps of the query.
   * @param sink  The sink of the query.
   */
  public HiveFlow( String name, String query, List<cascading.tap.Tap> sources, Tap sink)
    {
    this( name, new HiveDriverFactory(), query, sources, sink );
    }

  /**
   * Package private constructor which allows injecting a custom HiveDriverFactory during tests.
   *
   */
  HiveFlow( String name, HiveDriverFactory driverFactory, String query, List<cascading.tap.Tap> sources, Tap sink)
    {
    super( name, new HiveRiffle( driverFactory, query, sources, sink ) );
    }

  }
