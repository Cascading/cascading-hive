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

import cascading.flow.FlowProcess;
import cascading.scheme.NullScheme;
import cascading.scheme.Scheme;
import cascading.tap.Tap;
import cascading.tuple.TupleEntryCollector;
import cascading.tuple.TupleEntryIterator;

/**
 * Tap implementation used when there is no source or sink. One use case is DDL statements in which a table is created
 * (sink) out of nothing. You can use {HiveNullTap.DEV_NULL} or {HiveNullTap.DEV_ZERO} for reading and writing. They simply
 * don't do anything.
 */
public class HiveNullTap extends Tap
  {
  /** a no-op sink tap */
  public static final HiveNullTap DEV_NULL = new HiveNullTap( "/dev/null" );

  /** a no-op source tap */
  public static final HiveNullTap DEV_ZERO = new HiveNullTap( "/dev/zero" );

  /**
   * Identifier of the tap.
   */
  private final String identifier;

  /** null scheme */
  private final Scheme scheme = new NullScheme();

  /**
   * Constructs a new HiveNullTap with the given identifier.
   *
   * @param identifier
   */
  private HiveNullTap( String identifier )
    {
    this.identifier = identifier;
    }

  @Override
  public String getIdentifier()
    {
    return this.identifier;
    }

  @Override
  public TupleEntryIterator openForRead( FlowProcess flowProcess, Object object ) throws IOException
    {
    return null;
    }

  @Override
  public TupleEntryCollector openForWrite( FlowProcess flowProcess, Object object ) throws IOException
    {
    return null;
    }

  @Override
  public boolean createResource( Object conf ) throws IOException
    {
    return true;
    }

  @Override
  public boolean deleteResource( Object conf ) throws IOException
    {
    return false;
    }

  @Override
  public boolean resourceExists( Object conf ) throws IOException
    {
    return true;
    }

  @Override
  public long getModifiedTime( Object conf ) throws IOException
    {
    return System.currentTimeMillis();
    }

  @Override
  public Scheme getScheme()
    {
    return scheme;
    }
  }
