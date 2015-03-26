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

import java.util.Collection;
import java.util.Collections;
import java.util.Map;

import riffle.process.*;

/**
 * Class representing a step internal to Hive. The purpose is to report task level counters back to Cascading. Future
 * versions might give access to more internals of Hive.
 */
@riffle.process.Process
class HiveStep
  {
  /** unique id of the task */
  private final String id;

  /** counters associated with the task. */
  private final Map<String, Map<String,Long>> counters;

  /**
   * Constructs a new HiveStep instance.
   * @param id The id of the step.
   * @param counters Counters of the step.
   */
  HiveStep( String id, Map<String, Map<String, Long>> counters )
    {
    this.id = id;
    if( counters == null )
      this.counters = Collections.emptyMap();
    else
      this.counters = counters;
    }


  @DependencyIncoming
  public Collection incoming()
    {
    return Collections.emptyList();
    }

  @DependencyOutgoing
  public Collection outgoing()
    {
    return Collections.emptyList();
    }

  @ProcessStart
  public void start()
    {
    // NO OP
    }

  @ProcessComplete
  public void complete()
    {
    // NO OP
    }

  @ProcessStop
  public void stop()
    {
    // NO OP
    }

  @ProcessCounters
  public Map<String, Map<String, Long>> getCounters()
    {
    return counters;
    }

  @Override
  public boolean equals( Object object )
    {
    if( this == object )
      return true;
    if( object == null || getClass() != object.getClass() )
      return false;

    HiveStep that = (HiveStep) object;

    if( id != null ? !id.equals( that.id ) : that.id != null )
      return false;

    return true;
    }

  @Override
  public int hashCode()
    {
    return id != null ? id.hashCode() : 0;
    }

  @Override
  public String toString()
    {
    return "HiveStep{" +
      "id='" + id + '\'' +
      ", counters=" + counters +
      '}';
    }
  }
