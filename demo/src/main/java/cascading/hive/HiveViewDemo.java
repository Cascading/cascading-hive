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

package cascading.hive;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Collection;
import java.util.Properties;

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
import cascading.scheme.hadoop.TextDelimited;
import cascading.tap.Tap;
import cascading.tap.hadoop.Hfs;
import cascading.tap.hive.HivePartitionTap;
import cascading.tap.hive.HiveTableDescriptor;
import cascading.tap.hive.HiveTap;
import cascading.tap.hive.HiveViewAnalyzer;
import cascading.tuple.Fields;
import cascading.tuple.TupleEntry;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;

/**
 * Demo app showing view support.
 */
public class HiveViewDemo
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

    HiveTableDescriptor partitionedDescriptor = new HiveTableDescriptor( "mydb2", "mytable2", columnNames, columnTypes,
      partitionKeys, "\t" );

    HiveTap hiveTap = new HiveTap( partitionedDescriptor, partitionedDescriptor.toScheme() );

    Tap outputTap = new HivePartitionTap( hiveTap );
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

    Fields allFields = new Fields( columnNames );

    Pipe pipe = new Each( " echo ", allFields,
      new Echo( allFields ), Fields.RESULTS );

    Tap input = new Hfs( new TextDelimited( allFields ), "hdfs:/tmp/access.log" );

    Flow flow = new HadoopFlowConnector().connect( input, outputTap, pipe );

    flow.complete();

    String viewSelect = "select distinct customer from mydb2.mytable2 where region = 'ASIA'";
    String viewDef = "create or replace view customers_in_asia as " + viewSelect;
    HiveViewAnalyzer analyzer = new HiveViewAnalyzer();
    Collection<Tap> inputs = analyzer.asTaps( viewSelect );

    HiveFlow viewflow = new HiveFlow( "create view", viewDef, inputs );
    viewflow.complete();

    Class.forName( "org.apache.hadoop.hive.jdbc.HiveDriver" );

    Connection con = DriverManager.getConnection( "jdbc:hive://", "", "" );
    Statement stmt = con.createStatement();

    ResultSet rs = stmt.executeQuery( "select * from customers_in_asia " );

    System.out.println( "----------------------Hive JDBC--------------------------" );
    while( rs.next() )
      {
      System.out.println( "customer=" + rs.getString( 1 ) );
      }
    System.out.println( "---------------------------------------------------------" );
    stmt.close();
    con.close();

    }
  }
