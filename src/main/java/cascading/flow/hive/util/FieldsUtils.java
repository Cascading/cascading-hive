/*
 * Copyright (c) 2007-2009 Concurrent, Inc. All Rights Reserved.
 *
 * Project and contact information: http://www.cascading.org/
 *
 * This file is part of the Cascading project.
 *
 * Cascading is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Cascading is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Cascading.  If not, see <http://www.gnu.org/licenses/>.
 */

package cascading.flow.hive.util;

import java.util.List;

import cascading.tuple.Fields;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.plan.CreateTableDesc;

/**
 * Helper class for creting Fields instances based on FieldSchema information coming from hive.
 */
public class FieldsUtils
  {

  /**
   * Creates a new Fields instance based on the given List of FieldSchemas. If the list is <code>null</code> or empty
   * Fields.UNKNOWN is returned.
   *
   * @param cols a list fo FieldSchema instances
   * @return a cascading Fields instance, but never <code>null</code>.
   */
  public static Fields fromFieldSchemaList( List<FieldSchema> cols )
    {
    if( cols == null || cols.isEmpty() )
      return Fields.UNKNOWN;
    String[] names = new String[ cols.size() ];
    for( int index = 0; index < cols.size(); index++ )
      names[ index ] = cols.get( index ).getName();

    // TODO figure out meaningful type mapping here, if needed
    return new Fields( names );
    }

  }
