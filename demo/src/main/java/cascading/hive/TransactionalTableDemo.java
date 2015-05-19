package cascading.hive;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import org.apache.hadoop.mapred.JobConf;

import cascading.flow.Flow;
import cascading.flow.hadoop2.Hadoop2MR1FlowConnector;
import cascading.pipe.Pipe;
import cascading.property.AppProps;
import cascading.scheme.hadoop.TextDelimited;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tap.hadoop.Hfs;
import cascading.tap.hive.HivePartitionTap;
import cascading.tap.hive.HiveTableDescriptor;
import cascading.tap.hive.HiveTap;
import cascading.tap.hive.LockManager;
import cascading.tuple.Fields;
import cascading.tuple.TupleEntry;
import cascading.tuple.TupleEntryIterator;

import com.hotels.corc.cascading.OrcFile;

/**
 * Demo app showing transactional support. It reads data from a series of ORC base and delta files that represent a
 * number of inserts, updates, and deletes on the table. The data read back reflects the current state of the table.
 * <p/>
 * Expected output:
 *
 * <pre>
 * Cascading>>> fields: ['ROW__ID', 'id', 'message', 'continent', 'country' | RecordIdentifier, Integer, String, String, String]
 *   tuple: ['{originalTxn: 67, bucket: 0, row: 0}', '3', 'z', 'Asia', 'India']
 * Cascading>>> fields: ['ROW__ID', 'id', 'message', 'continent', 'country' | RecordIdentifier, Integer, String, String, String]
 *   tuple: ['{originalTxn: 67, bucket: 0, row: 2}', '1', 'updated', 'Asia', 'India']
 * </pre>
 */
public class TransactionalTableDemo {

  public static void main(String[] args) throws Exception {
    //     @formatter:off
    Class.forName( "org.apache.hive.jdbc.HiveDriver" );

    Connection con = getConnection();
    Statement stmt = con.createStatement();

    stmt.executeUpdate(
        "CREATE TABLE test_table ( id int, message string ) " +
            "  PARTITIONED BY ( continent string, country string ) " +
            "  CLUSTERED BY (id) INTO 1 BUCKETS " +
            "  STORED AS ORC " +
            "  TBLPROPERTIES ('transactional' = 'true')"
        );
    System.out.println("Created table.");

    stmt.executeUpdate(
        "INSERT INTO TABLE test_table PARTITION (continent = 'Asia', country = 'India') " +
            "VALUES (1, 'x'), (2, 'y'), (3, 'z')"
        );
    System.out.println("Inserted rows.");

    stmt.executeUpdate("DELETE FROM test_table WHERE id = 2");
    System.out.println("Deleted row.");

    stmt.executeUpdate("UPDATE test_table SET message = 'updated' WHERE id = 1");
    System.out.println("Updated row.");

    stmt.close();
    con.close();
    // @formatter:on

    Properties properties = new Properties();
    AppProps.setApplicationName(properties, "cascading hive transactional demo");

    JobConf jobConf = createJobConf();

    HiveTableDescriptor.Factory factory = new HiveTableDescriptor.Factory(jobConf);
    HiveTableDescriptor tableDescriptor = factory.newInstance("test_table");

    OrcFile scheme = OrcFile.source().schema(tableDescriptor.toTypeInfo()).prependRowId().build();
    HiveTap hiveTap = new HiveTap(tableDescriptor, scheme);
    Tap partitionTap = new HivePartitionTap(hiveTap);

    Fields columnFields = scheme.getSourceFields();
    Fields partitionFields = tableDescriptor.getPartition().getPartitionFields();
    Fields allFields = Fields.join(columnFields, partitionFields);
    Tap sink = new Hfs(new TextDelimited(allFields), "/tmp/data-read-from-transactional", SinkMode.REPLACE);

    Flow flow = new Hadoop2MR1FlowConnector().connect(partitionTap, sink, new Pipe("identity"));

    LockManager lockManager = new LockManager.Builder().addTable(tableDescriptor).managedFlow(flow).build();
    flow.addListener(lockManager);
    flow.complete();

    TupleEntryIterator tupleEntryIterator = sink.openForRead(flow.getFlowProcess());

    while (tupleEntryIterator.hasNext()) {
      TupleEntry tupleEntry = tupleEntryIterator.next();
      System.out.println("Cascading>>> " + tupleEntry);
    }
    tupleEntryIterator.close();
  }

  private static JobConf createJobConf() {
    JobConf jobConf = new JobConf();
    String metastoreUris = System.getProperty("hive.metastore.uris", "");
    if (!metastoreUris.isEmpty()) {
      jobConf.set("hive.metastore.uris", metastoreUris);
    }
    return jobConf;
  }

  private static Connection getConnection() throws SQLException {
    String url = System.getProperty("hive.server.url", "jdbc:hive2://");
    String user = System.getProperty("hive.server.user", "");
    String password = System.getProperty("hive.server.password", "");
    Connection con = DriverManager.getConnection(url, user, password);
    return con;
  }

}
