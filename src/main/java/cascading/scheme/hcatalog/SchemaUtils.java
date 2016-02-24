/*
* Copyright (c) 2007-2016 Concurrent, Inc. All Rights Reserved.
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

package cascading.scheme.hcatalog;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.hive.hcatalog.data.schema.HCatFieldSchema;
import org.apache.hive.hcatalog.data.schema.HCatSchema;

import cascading.tuple.Fields;

/**
 * A set of static convenient methods for working with {@link HCatSchema}.
 */
final class SchemaUtils {

  private SchemaUtils() {
  }

  /**
   * Returns a {@link Set} of lower-case column names from the given {@link Fields}.
   * <p>
   * Column names are case-insensitive unique {@link String strings}.
   * </p>
   *
   * @param fields Cascading {@linkplain Fields fields} that should map to column names.
   * @throws {@code IllegalArgumentException} If {@code fields} contains ignore-case duplicates.
   */
  public static Set<String> getLowerCaseFieldNames(Fields fields) {
    Set<String> names = new HashSet<>(fields.size());
    Set<String> duplicates = new HashSet<>(fields.size());
    for (Comparable<?> name : fields) {
      String lowerCaseName = name.toString().toLowerCase();
      if (!names.add(lowerCaseName)) {
        duplicates.add(lowerCaseName);
      }
    }
    if (duplicates.size() > 0) {
      throw new IllegalArgumentException(
          String.format("Duplicate field name(s) found %s. HCatalog is case insensitive.", duplicates));
    }
    return Collections.unmodifiableSet(names);
  }

  /**
   * Returns a {@link HCatSchema} that describe a source Hive table, containing both the given of partition columns and
   * the given set of data columns.
   *
   * @param partitionColumns Partition columns of the Hive table.
   * @param dataColumns Data columns of the Hive table.
   * @param fields A set of {@link Fields fields} that must map to both partition and data columns.
   * @return A {@link HCatScheme} with both partition and data columns.
   * @throws {@code IllegalArgumentException} If at least one {@code partitionColumns} or {@code dataColumns} is missing
   *           or not present in {@code fields}.
   */
  public static HCatSchema getSourceSchema(HCatSchema partitionColumns, HCatSchema dataColumns, Fields fields) {
    Set<String> names = getLowerCaseFieldNames(fields);
    return getSchema(partitionColumns, dataColumns, names);
  }

  /**
   * Returns a {@link HCatSchema} that describe a sink Hive table, containing both the given set of partition columns
   * and the given set of data columns.
   * <p>
   * All partition columns must be present in {@code fields}
   * </p>
   *
   * @param partitionColumns Partition columns of the Hive table.
   * @param dataColumns Data columns of the Hive table.
   * @param fields A set of {@link Fields fields} that must map to both partition and data columns.
   * @return A {@link HCatScheme} with both partition and data columns.
   * @throws {@code IllegalArgumentException} If at least one {@code partitionColumns} or {@code dataColumns} is missing
   *           or not present in {@code fields}.
   */
  public static HCatSchema getSinkSchema(HCatSchema partitionColumns, HCatSchema dataColumns, Fields fields) {
    Set<String> names = getLowerCaseFieldNames(fields);

    Set<String> remainingNames = new HashSet<>(partitionColumns.getFieldNames());
    for (String name : partitionColumns.getFieldNames()) {
      if (names.contains(name)) {
        remainingNames.remove(name);
      }
    }
    if (remainingNames.size() > 0) {
      throw new IllegalArgumentException(String.format("Partition columns are mandatory but %s %s not specified.",
          remainingNames, remainingNames.size() == 1 ? "was" : "were"));
    }

    return getSchema(partitionColumns, dataColumns, names);

  }

  /**
   * Returns a {@link HCatSchema} that describe the Hive table, containing both the given set of partition columns and
   * the given set of data columns.
   * <p>
   * Both {@code partitionColumns} and {@code dataColumns} are validated against the expected set of column
   * {@code names} .
   * </p>
   *
   * @param partitionColumns Partition columns of the Hive table.
   * @param dataColumns Data columns of the Hive table.
   * @param names A {@link Set} of expected column names.
   * @return A {@link HCatScheme} with both partition and data columns.
   * @throws {@code IllegalArgumentException} If at least one {@code partitionColumns} or {@code dataColumns} is missing
   *           or not present in {@code names}.
   */
  private static HCatSchema getSchema(HCatSchema partitionColumns, HCatSchema dataColumns, Set<String> names) {
    List<HCatFieldSchema> columns = new ArrayList<>(names.size());

    Set<String> remainingNames = new HashSet<>(names);
    for (HCatFieldSchema column : partitionColumns.getFields()) {
      if (remainingNames.remove(column.getName())) {
        columns.add(column);
      }
    }
    for (HCatFieldSchema column : dataColumns.getFields()) {
      if (remainingNames.remove(column.getName())) {
        columns.add(column);
      }
    }
    if (remainingNames.size() > 0) {
      throw new IllegalArgumentException(String
          .format("The following columns were specified but do not exist in the HCatalog table: %s.", remainingNames));
    }

    return new HCatSchema(columns);
  }

}
