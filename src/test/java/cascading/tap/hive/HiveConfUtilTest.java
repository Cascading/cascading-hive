package cascading.tap.hive;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.junit.Test;

public class HiveConfUtilTest {

  @Test
  public void nullConfiguration() {
    HiveConf hiveConf = HiveConfUtil.toHiveConf(null);
    assertNotNull(hiveConf);
  }
  
  @Test
  public void copiesConfiguration() {
    Configuration configuration = new Configuration();
    configuration.set("X", "Y");
    HiveConf hiveConf = HiveConfUtil.toHiveConf(configuration);
    assertEquals("Y", hiveConf.get("X"));
  }
  
}
