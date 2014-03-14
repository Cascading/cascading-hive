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

package cascading.flow.hive.utils;

import java.util.ArrayList;
import java.util.List;

import cascading.flow.hive.util.FieldsUtils;
import cascading.tuple.Fields;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 *
 */
public class FieldsUtilsTest
  {
  @Test
  public void testCreateFieldsWithEmptyList()
    {
    Fields result = FieldsUtils.fromFieldSchemaList( new ArrayList<FieldSchema>(  ) );
    assertEquals( Fields.UNKNOWN, result );
    }

  @Test
  public void testCreateFieldsWithNull()
    {
    Fields result = FieldsUtils.fromFieldSchemaList( null );
    assertEquals( Fields.UNKNOWN, result );
    }

  @Test
  public void testCreateFieldsWithFieldSchemas()
    {
    List<FieldSchema> fieldSchemas = new ArrayList<FieldSchema>(  );
    fieldSchemas.add( new FieldSchema( "id", "int", "/* no comment */" ) );
    fieldSchemas.add( new FieldSchema( "firstname", "string", "/* no comment */" ) );
    fieldSchemas.add( new FieldSchema( "lastname", "string", "/* no comment */" ) );
    Fields result = FieldsUtils.fromFieldSchemaList( fieldSchemas );
    assertEquals( new Fields( "id", "firstname", "lastname" ), result );
    }

  }
