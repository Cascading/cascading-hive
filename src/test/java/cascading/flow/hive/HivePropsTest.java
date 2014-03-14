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

package cascading.flow.hive;

import java.util.Properties;

import static org.junit.Assert.*;
import org.junit.Test;

/**
 * Tests for HiveProps.
 */
public class HivePropsTest
  {
  @Test
  public void testHiveProps()
    {
    Properties props = new Properties(  );
    String expected =  "thrift://localhost:9083";
    new HiveProps().setMetastoreUris( expected ).addPropertiesTo( props );
    assertEquals( props.get( HiveProps.METASTORE_URIS ), expected );
    }

  }
