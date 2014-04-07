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

package cascading.hive;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Properties;

import cascading.cascade.Cascade;
import cascading.cascade.CascadeConnector;
import cascading.flow.Flow;
import cascading.flow.FlowProcess;
import cascading.flow.hadoop.HadoopFlowConnector;
import cascading.flow.hive.HiveFlow;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.property.AppProps;
import cascading.scheme.NullScheme;
import cascading.tap.Tap;
import cascading.tap.hadoop.Hfs;
import cascading.tap.hive.HiveTableDescriptor;
import cascading.tap.hive.HiveTap;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.mapred.JobConf;

import static cascading.tap.SinkMode.REPLACE;

/**
 * Demo Application using HiveFlows which creates three tables, loads data, converts them in cascading and reads it
 * back via JDBC.
 */
public class HiveDemo
  {
  private static String dualTableInputFile = "file://" + System.getProperty( "user.dir" ) + "/src/main/resources/data.txt";

  public static void main( String[] args ) throws Exception
    {
    Properties properties = new Properties();
    AppProps.setApplicationName( properties, "cascading hive integration demo" );

    // descriptor of the dual table.
    HiveTableDescriptor dualTableDescriptor = new HiveTableDescriptor( "dual", new String[]{"value"}, new String[]{
      "string"} );
    // create HiveTap based on the descriptor
    HiveTap dualTap = new HiveTap( dualTableDescriptor, dualTableDescriptor.toScheme(), REPLACE, true );

    // load local data into dual. The table will be created as needed.
    HiveFlow loadDataFlow = new HiveFlow( "load data into dual",
      String.format( "load data local inpath '%s' overwrite into table dual", dualTableInputFile ),
      Arrays.<Tap>asList( new Hfs( new NullScheme(), dualTableInputFile ) ), dualTap );
    // load data from local fs into the hive table

    // describe a second table: keyvalue
    HiveTableDescriptor keyValueDescriptor = new HiveTableDescriptor( "keyvalue", new String[]{"key", "value"},
      new String[]{"string", "string"} );

    HiveTap keyvalueTap = new HiveTap( keyValueDescriptor, keyValueDescriptor.toScheme(), REPLACE, true );

    // populate data in keyvalue by selecting data from dual
    HiveFlow selectFlow = new HiveFlow( "select data from dual into keyvalue",
      "insert overwrite table keyvalue select 'Hello' as key, 'hive!' as value from dual ",
      Arrays.<Tap>asList( dualTap ), keyvalueTap );

    // describe a third table, similar to keyvalue. This will be used as a sink in a pure cascading flow
    HiveTableDescriptor keyValueDescriptor2 = new HiveTableDescriptor( "keyvalue2", new String[]{"key", "value"},
      new String[]{"string", "string"} );

    HiveTap keyvalue2Tap = new HiveTap( keyValueDescriptor2, keyValueDescriptor2.toScheme(), REPLACE, true );

    // simple function we use in a pure cascading flow
    class Upper extends BaseOperation implements Function
      {
      public Upper( Fields fieldDeclaration )
        {
        super( 2, fieldDeclaration );
        }

      @Override
      public void operate( FlowProcess flowProcess, FunctionCall functionCall )
        {
        TupleEntry argument = functionCall.getArguments();
        String key = argument.getString( 0 ).toUpperCase();
        String value = argument.getString( 1 ).toUpperCase();
        Tuple result = new Tuple( key, value );
        functionCall.getOutputCollector().add( result );
        }
      }
    // create a pure cascading flow, that reads from the second table, converts everything to upper case and writes it into the third table

    Pipe upperPipe = new Each( "uppercase kv -> kv2 ", keyvalueTap.getSinkFields(),
      new Upper( keyvalueTap.getSinkFields() ), Fields.RESULTS );
    Flow<?> pureCascadingFlow = new HadoopFlowConnector().connect( keyvalueTap, keyvalue2Tap, upperPipe );

    // run all of the above flows, cascading will figure out the right order of execution
    Cascade cascade = new CascadeConnector( properties ).connect( pureCascadingFlow,
      loadDataFlow, selectFlow );

    cascade.writeDOT( "hivedemo.dot" );
    cascade.complete();

    // finally read the data from the table, that was filled by cascading via the Hive JDBC driver
    Class.forName( "org.apache.hadoop.hive.jdbc.HiveDriver" );

    // change the url, if you use hiveserver2
    Connection con = DriverManager.getConnection( "jdbc:hive://", "", "" );
    Statement stmt = con.createStatement();

    ResultSet rs = stmt.executeQuery( "select key, value from keyvalue2" );
    System.out.println( "----------------------Hive JDBC--------------------------" );
    while( rs.next() )
      {
      System.out.printf( "data from hive table copy: key=%s,value=%s\n", rs.getString( 1 ), rs.getString( 2 ) );
      }
    System.out.println( "---------------------------------------------------------" );
    stmt.close();
    con.close();
    }

  }
