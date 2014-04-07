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
import java.util.Properties;

import cascading.flow.Flow;
import cascading.flow.FlowProcess;
import cascading.flow.hadoop.HadoopFlowConnector;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.property.AppProps;
import cascading.scheme.hadoop.TextDelimited;
import cascading.tap.Tap;
import cascading.tap.hadoop.Hfs;
import cascading.tap.hive.HivePartitionTap;
import cascading.tap.hive.HiveTableDescriptor;
import cascading.tap.hive.HiveTap;
import cascading.tuple.Fields;
import cascading.tuple.TupleEntry;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;

/**
 *
 */
public class HivePartitionDemo
  {

  private static String accessLog = "file://" + System.getProperty( "user.dir" ) + "/src/main/resources/access.log";

  public static void main( String[] args ) throws Exception
    {
    Properties properties = new Properties();
    AppProps.setApplicationName( properties, "cascading hive partitioning demo" );

    JobConf jobConf = new JobConf();

    FileSystem fs = FileSystem.get( jobConf );
    fs.copyFromLocalFile( false, true, new Path( accessLog ), new Path( "/tmp/access.log" ) );

    String[] columnNames = new String[]{"ts", "customer", "bucket", "operation", "key", "region"};
    String[] columnTypes = new String[]{"timestamp", "string", "string", "string", "string", "string"};
    String[] partitionKeys = new String[]{"region"};

    HiveTableDescriptor partitionedDescriptor = new HiveTableDescriptor( "mydb", "mytable", columnNames, columnTypes,
      partitionKeys, "\t" );

    HiveTap hiveTap = new HiveTap( partitionedDescriptor, partitionedDescriptor.toScheme() );

    Tap outputTap = new HivePartitionTap( hiveTap );

    Fields allFields = new Fields( columnNames );

    Tap input = new Hfs( new TextDelimited( allFields), "hdfs:/tmp/access.log" );

    class Echo extends BaseOperation implements Function
      {
      public Echo( Fields fieldDeclaration )
        {
        super( 2, fieldDeclaration );
        }

      @Override
      public void operate( FlowProcess flowProcess, FunctionCall functionCall )
        {
        TupleEntry argument = functionCall.getArguments();
        functionCall.getOutputCollector().add( argument.getTuple() );
        }
      }

    Pipe pipe = new Each( " echo ", allFields,
      new Echo( allFields ), Fields.RESULTS );

    Flow flow = new HadoopFlowConnector().connect( input, outputTap, pipe );

    flow.complete();

    Class.forName( "org.apache.hadoop.hive.jdbc.HiveDriver" );

    // change the url, if you use hiveserver2
    Connection con = DriverManager.getConnection( "jdbc:hive://", "", "" );
    Statement stmt = con.createStatement();

    ResultSet rs = stmt.executeQuery( "select * from mydb.mytable where region = 'ASIA' " );

    String[] names = partitionedDescriptor.getColumnNames();
    System.out.println( "----------------------Hive JDBC--------------------------" );
    while( rs.next() )
      {
      StringBuffer buf = new StringBuffer();
      for( int i = 0; i < names.length; i++ )
        {
        String name = names[ i ];
        buf.append( name ).append( "=" ).append( rs.getObject( i + 1 ) ).append( ", " );
        }
      System.out.println( buf.toString() );
      }
    System.out.println( "---------------------------------------------------------" );
    stmt.close();
    con.close();
    }

  }
