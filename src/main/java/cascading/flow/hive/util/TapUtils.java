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

import cascading.scheme.NullScheme;
import cascading.scheme.hadoop.TextDelimited;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tap.hadoop.Hfs;
import cascading.tuple.Fields;

/**
 *  Utility class for creating Taps.
 */
public class TapUtils
  {

  /** The default delimiter used by the hive delimited format. */
  public static final String HIVE_DEFAULT_DELIMITER = "\1";

  public static Tap createTap( String path, Fields fields, SinkMode sinkMode, String delimiter)
    {
    if (delimiter == null )
      delimiter = HIVE_DEFAULT_DELIMITER;
    if ( fields == Fields.UNKNOWN )
      return new Hfs( new NullScheme(), path, sinkMode);
    else
      return new Hfs( new TextDelimited( fields, delimiter ), path, sinkMode);
    }

  public static Tap createTap( String path, Fields fields, SinkMode sinkMode )
    {
    return createTap( path, fields, sinkMode, HIVE_DEFAULT_DELIMITER );
    }

  }
