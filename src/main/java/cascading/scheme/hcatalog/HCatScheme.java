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

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hive.hcatalog.common.HCatConstants;
import org.apache.hive.hcatalog.common.HCatUtil;
import org.apache.hive.hcatalog.data.DefaultHCatRecord;
import org.apache.hive.hcatalog.data.HCatRecord;
import org.apache.hive.hcatalog.data.schema.HCatSchema;
import org.apache.hive.hcatalog.mapreduce.HCatBaseOutputFormat;
import org.apache.hive.hcatalog.mapreduce.HCatInputFormat;
import org.apache.hive.hcatalog.mapreduce.HCatOutputFormat;
import org.apache.hive.hcatalog.mapreduce.HCatTableInfo;
import org.apache.hive.hcatalog.mapreduce.OutputJobInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cascading.flow.FlowProcess;
import cascading.hadoop.mapred.OutputCommitterWrapper;
import cascading.scheme.Scheme;
import cascading.scheme.SinkCall;
import cascading.scheme.SourceCall;
import cascading.scheme.hcatalog.HCatScheme.SourceContext;
import cascading.tap.Tap;
import cascading.tap.hcatalog.HCatTap;
import cascading.tuple.Fields;
import cascading.tuple.TupleEntry;

/**
 * A {@link Scheme} to be used in conjunction with {@link HCatTap}.
 * <p>
 * Use this {@link Scheme} to specify the {@link Fields} to read from a Hive table. {@code HCatScheme} uses HCatalog to
 * perform metadata validations and simplify the processing of the records.
 * </p>
 * <p>
 * Each field is mapped to a column of the same name in the Hive table so the {@linkplain Fields fields} type must match
 * the Hive column type.
 * </p>
 * <p>
 * Note that Cascading {@link Fields} are case-sensitive whereas Hive column names are case-insensitive. This
 * {@link Scheme} makes sure that no duplicate {@linkplain Fields field} names are selected. If two or more
 * {@linkplain Fields fields} have the same case-insensitive name then the {@link Scheme} will throw an
 * {@link Exception}.
 * </p>
 *
 * @since 2.1
 */
public class HCatScheme extends Scheme<Configuration, RecordReader, OutputCollector, SourceContext, HCatSchema> {

  private static final long serialVersionUID = 1L;

  private static final Logger LOG = LoggerFactory.getLogger(HCatScheme.class);

  /**
   * Creates a new {@link HCatScheme}.
   *
   * @param fields Selected {@link Fields}. Field names and type must match their equivalent in the Hive table.
   * @throws {@link IllegalArgumentException} If duplicate case-insensitive names are set in {@code fields}.
   */
  public HCatScheme(Fields fields) {
    super(fields, fields);
    SchemaUtils.getLowerCaseFieldNames(fields); // fail-fast on case insensitive duplicates
  }

  @Override
  public void sourceConfInit(FlowProcess<? extends Configuration> flowProcess,
      Tap<Configuration, RecordReader, OutputCollector> tap, Configuration conf) {
    try {
      HCatSchema partitionColumns = HCatInputFormat.getPartitionColumns(conf);
      HCatSchema dataColumns = HCatInputFormat.getDataColumns(conf);
      HCatSchema schema = SchemaUtils.getSourceSchema(partitionColumns, dataColumns, getSourceFields());
      // This is equivalent to HCatBaseInputFormat.setOutputSchema
      conf.set(HCatConstants.HCAT_KEY_OUTPUT_SCHEMA, HCatUtil.serialize(schema));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void sourcePrepare(FlowProcess<? extends Configuration> flowProcess,
      SourceCall<SourceContext, RecordReader> sourceCall)
    throws IOException {
    Configuration conf = flowProcess.getConfig();

    HCatSchema partitionColumns = HCatInputFormat.getPartitionColumns(conf);
    HCatSchema dataColumns = HCatInputFormat.getDataColumns(conf);
    HCatSchema schema = SchemaUtils.getSourceSchema(partitionColumns, dataColumns, getSourceFields());

    WritableComparable<?> key = (WritableComparable<?>) sourceCall.getInput().createKey();
    HCatRecord record = (HCatRecord) sourceCall.getInput().createValue();
    sourceCall.setContext(new SourceContext(schema, key, record));
  }

  @Override
  public boolean source(FlowProcess<? extends Configuration> flowProcess,
      SourceCall<SourceContext, RecordReader> sourceCall)
    throws IOException {
    WritableComparable<?> key = sourceCall.getContext().key;
    HCatRecord record = sourceCall.getContext().record;

    if (!sourceCall.getInput().next(key, record)) {
      return false;
    }

    HCatSchema schema = sourceCall.getContext().schema;
    TupleEntry entry = sourceCall.getIncomingEntry();

    Fields fields = getSourceFields();
    for (Comparable<?> field : fields) {
      Object value = record.get(field.toString(), schema);
      entry.setObject(field, value);
    }

    return true;
  }

  @Override
  public void sinkConfInit(FlowProcess<? extends Configuration> flowProcess,
      Tap<Configuration, RecordReader, OutputCollector> tap, Configuration conf) {
    OutputCommitterWrapper.setOutputCommitter(conf);

    try {
      OutputJobInfo outputJobInfo = HCatBaseOutputFormat.getJobInfo(conf);
      HCatTableInfo tableInfo = outputJobInfo.getTableInfo();
      HCatSchema schema = SchemaUtils.getSinkSchema(tableInfo.getPartitionColumns(), tableInfo.getDataColumns(),
          getSinkFields());
      HCatOutputFormat.setSchema(conf, schema);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void sinkPrepare(FlowProcess<? extends Configuration> flowProcess,
      SinkCall<HCatSchema, OutputCollector> sinkCall)
    throws IOException {
    Configuration conf = flowProcess.getConfig();
    OutputCommitterWrapper.unsetOutputCommitter(conf);

    OutputJobInfo outputJobInfo = HCatBaseOutputFormat.getJobInfo(conf);
    HCatTableInfo tableInfo = outputJobInfo.getTableInfo();
    HCatSchema schema = SchemaUtils.getSinkSchema(tableInfo.getPartitionColumns(), tableInfo.getDataColumns(),
        getSinkFields());
    sinkCall.setContext(schema);
  }

  @SuppressWarnings("unchecked")
  @Override
  public void sink(FlowProcess<? extends Configuration> flowProcess, SinkCall<HCatSchema, OutputCollector> sinkCall)
    throws IOException {
    HCatSchema schema = sinkCall.getContext();
    TupleEntry entry = sinkCall.getOutgoingEntry();
    HCatRecord record = new DefaultHCatRecord(schema.size());

    Fields fields = getSinkFields();
    for (Comparable<?> field : fields) {
      Object value = entry.getObject(field);
      record.set(field.toString(), schema, value);
    }

    sinkCall.getOutput().collect(null, record);
  }

  static class SourceContext {
    private final HCatSchema schema;
    private final WritableComparable<?> key;
    private final HCatRecord record;

    SourceContext(HCatSchema schema, WritableComparable<?> key, HCatRecord record) {
      this.schema = schema;
      this.key = key;
      this.record = record;
    }

  }

}
