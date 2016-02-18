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
package cascading.hive;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.hadoop.conf.Configuration;

import cascading.flow.Flow;
import cascading.flow.FlowDef;
import cascading.flow.hadoop.util.HadoopUtil;
import cascading.flow.hadoop2.Hadoop2MR1FlowConnector;
import cascading.operation.Debug;
import cascading.operation.Debug.Output;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.scheme.hadoop.TextDelimited;
import cascading.tap.Tap;
import cascading.tap.hadoop.Hfs;
import cascading.tap.hive.HivePartitionTap;
import cascading.tap.hive.HiveTableDescriptor;
import cascading.tap.hive.HiveTap;
import cascading.tuple.Fields;

/**
 * Demo app showing selective partition reading support.
 */
public class HivePartitionSelectiveReadDemo {

  public static void main(String[] args) throws Exception {
    createHiveTableWithData();
    testFlowReadOnePartition();
  }

  private static void createHiveTableWithData() throws ClassNotFoundException, SQLException {
    Class.forName("org.apache.hive.jdbc.HiveDriver");
    try (Connection connection = DriverManager.getConnection("jdbc:hive2://", "", "");
        Statement statement = connection.createStatement()) {
      statement.execute("drop table if exists test_table purge");
      statement.execute("create table test_table (foo string) partitioned by (bar string)"
          + " row format delimited fields terminated by '\\t' stored as textfile" + " location '/tmp/hpt-src-"
          + UUID.randomUUID() + "'");
      statement.execute("set hive.exec.dynamic.partition.mode=nonstrict");
      statement.execute("insert overwrite table test_table partition (bar) "
          + "values ('foo1', 'bar1'),  ('foo2', 'bar2')");
    }
  }

  private static void testFlowReadOnePartition() throws IOException {
    HiveTableDescriptor tableDesc = new HiveTableDescriptor("default", "test_table", new String[] { "foo", "bar" },
        new String[] { "string", "string" }, new String[] { "bar" }, "\t");
    HiveTap parent = new HiveTap(tableDesc, tableDesc.toScheme());
    List<List<String>> selectedPartitionValues = Arrays.asList(Arrays.asList("bar1"));
    Tap source = new HivePartitionTap(parent, selectedPartitionValues);

    Tap sink = new Hfs(new TextDelimited(new Fields("foo", "bar")), "/tmp/hpt-tgt-" + UUID.randomUUID());

    Pipe head = new Pipe("pipe");
    Pipe tail = new Each(head, new Debug(Output.STDOUT, true));

    Configuration conf = new Configuration();
    Map<Object, Object> properties = HadoopUtil.createProperties(conf);

    FlowDef flowDef = FlowDef.flowDef().addSource(head, source).addTailSink(tail, sink);
    Flow flow = new Hadoop2MR1FlowConnector(properties).connect(flowDef);
    flow.complete();
    flow.cleanup();
  }

}
