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

package cascading.hive;

import java.lang.reflect.Type;
import java.net.URL;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import cascading.flow.Flow;
import cascading.flow.FlowProcess;
import cascading.flow.hadoop2.Hadoop2MR1FlowConnector;
import cascading.operation.aggregator.Count;
import cascading.pipe.Every;
import cascading.pipe.GroupBy;
import cascading.pipe.Pipe;
import cascading.property.AppProps;
import cascading.scheme.hadoop.TextDelimited;
import cascading.scheme.hadoop.TextLine;
import cascading.scheme.hcatalog.HCatScheme;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tap.hadoop.Hfs;
import cascading.tap.hcatalog.HCatTap;
import cascading.tap.hive.HivePartitionTap;
import cascading.tap.hive.HiveTableDescriptor;
import cascading.tap.hive.HiveTap;
import cascading.tuple.Fields;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;

/**
 * Demo app showing {@link HCatTap} as source and sink.
 */
public class HCatTapDemo
  {

  private static URL ACCESS_LOG = HCatTapDemo.class.getResource( "/access.log" );
  private static String DATABASE_NAME = "default";
  private static String TABLE_NAME = "access_log";

  public static void main( String[] args ) throws Exception
    {
    Class.forName( "org.apache.hive.jdbc.HiveDriver" );

    Properties properties = new Properties();
    AppProps.setApplicationName( properties, "Cascading HCatTap demo" );

    JobConf jobConf = createJobConf();
    FileSystem fs = FileSystem.get( jobConf );
 
    Path accessLogPath = new Path( "/tmp/access.log" );
    System.out.println( "Copying file '"+ACCESS_LOG+"' to '"+accessLogPath+"'" );
    fs.copyFromLocalFile( false, true, new Path( ACCESS_LOG.toURI() ), accessLogPath );

    createTable();

    // Load file into a hive table
    Fields logFields = new Fields( new String[]{"ts", "customer", "bucket", "operation", "key", "region"},
        new Type[]{String.class, String.class, String.class, String.class, String.class, String.class} );
    Tap logFile = new Hfs( new TextDelimited( logFields ), "hdfs:/tmp/access.log" );
    // Note that default.access_log has been created as a partitioned table by region but this is transparent
    // to the tap. Partitions will be determined at run time using the values of the 'region' field and
    // automatically created after sinking the data
    Tap logTable = new HCatTap( new HCatScheme( logFields ), DATABASE_NAME, TABLE_NAME );

    Pipe copyLogFilePipe = new Pipe( "Copy log file to Hive table" );

    Flow copyLogFileFlow = new Hadoop2MR1FlowConnector().connect( logFile, logTable, copyLogFilePipe );
    copyLogFileFlow.complete();

    // Output sample data
    executeQuery( "SELECT * FROM "+DATABASE_NAME+"."+TABLE_NAME+" LIMIT 20" );

    // Filter log data: in this example we only read the 'ts' and 'region' columns in 'ASIA' and 'EU' partitions
    Fields projectedLogFields = new Fields( new String[]{"ts", "region"}, new Type[]{String.class, String.class} );
    Tap filteredLogTable = new HCatTap( new HCatScheme( projectedLogFields ), DATABASE_NAME, TABLE_NAME, "region='ASIA' OR region='EU'" );

    Fields region = new Fields( "region", String.class );
    Fields total = new Fields ( "count", Long.class );
    Fields filteredLogFields = region.append( total );
    Tap filteredLogFile = new Hfs( new TextDelimited( filteredLogFields ), "hdfs:/tmp/filtered_access.log", SinkMode.REPLACE );

    Pipe filteredLogFilePipe = new Pipe( "Filter log table and count by region" );
    filteredLogFilePipe = new GroupBy( filteredLogFilePipe, region );
    filteredLogFilePipe = new Every( filteredLogFilePipe, new Count(total), filteredLogFields );

    Flow filterLogFileFlow = new Hadoop2MR1FlowConnector().connect( filteredLogTable, filteredLogFile, filteredLogFilePipe );
    filterLogFileFlow.complete();

    // Output selected data
    executeQuery( "SELECT COUNT(*), region FROM "+DATABASE_NAME+"."+TABLE_NAME+" WHERE region='ASIA' OR region='EU' GROUP BY region" );
    }

  private static JobConf createJobConf()
    {
    JobConf jobConf = new JobConf();
    String metastoreUris = System.getProperty( "hive.metastore.uris", "" );
    if( !metastoreUris.isEmpty() )
      jobConf.set( "hive.metastore.uris", metastoreUris );
    return jobConf;
    }

  private static Connection getConnection() throws SQLException
    {
    String url = System.getProperty( "hive.server.url", "jdbc:hive2://" );
    String user = System.getProperty( "hive.server.user", "" );
    String password = System.getProperty( "hive.server.password", "" );
    Connection con = DriverManager.getConnection( url, user, password );
    return con;
    }

  private static void executeQuery( String hql ) throws Exception
    {
    Connection con = getConnection();
    Statement stmt = con.createStatement();

    ResultSet rs = stmt.executeQuery( hql );
    ResultSetMetaData rsmd = rs.getMetaData();

    System.out.println( "----------------------Hive JDBC--------------------------" );
    System.out.println( "HQL = " + hql );
    while( rs.next() )
      {
      StringBuffer buf = new StringBuffer( "JDBC>>> " );
      for( int i = 1; i <= rsmd.getColumnCount(); i++ )
        {
        buf.append( rsmd.getColumnName( i ) ).append( "=" ).append( rs.getObject( i ) ).append( ", " );
        }
      System.out.println( buf.toString() );
      }
    System.out.println( "---------------------------------------------------------" );

    stmt.close();
    con.close();
    }

  private static void createTable() throws Exception
    {
    Connection con = getConnection();
    Statement stmt = con.createStatement();

    stmt.executeUpdate( "CREATE DATABASE IF NOT EXISTS "+DATABASE_NAME );

    String createTable = new StringBuilder()
        .append( " CREATE TABLE IF NOT EXISTS "+DATABASE_NAME+"."+TABLE_NAME+" (" )
        .append( "   ts STRING, " )
        .append( "   customer STRING, " )
        .append( "   bucket STRING, " )
        .append( "   operation STRING, " )
        .append( "   key STRING " )
        .append( " ) " )
        .append( " PARTITIONED BY (region STRING) " )
        .append( " STORED AS ORC  " )
        .toString();
    stmt.executeUpdate( createTable );

    stmt.executeUpdate( "TRUNCATE TABLE "+DATABASE_NAME+"."+TABLE_NAME );

    String dropPartitions = new StringBuilder()
        .append( " ALTER TABLE "+DATABASE_NAME+"."+TABLE_NAME+" DROP IF EXISTS " )
        .append( " PARTITION (region='ASIA'), " )
        .append( " PARTITION (region='EU'), " )
        .append( " PARTITION (region='US') " )
        .toString();
    stmt.executeUpdate( dropPartitions );

    stmt.close();
    con.close();
    }
  }
