package cascading.tap.hive;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;

final class HiveConfUtil {

  static HiveConf toHiveConf(Configuration conf) {
    HiveConf hiveConf = new HiveConf();
    if (conf != null) {
      hiveConf.addResource(conf);
    }
    return hiveConf;
  }

}
