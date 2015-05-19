package cascading.tap.hive;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;

final class HiveConfFactory {

  private static HiveConf hiveConf;
  
  static synchronized HiveConf getHiveConf(Configuration conf) {
    if (hiveConf == null) {
      hiveConf = new HiveConf();
    }
    if (conf != null) {
      hiveConf.addResource(conf);
    }
    return hiveConf;
  }

}
