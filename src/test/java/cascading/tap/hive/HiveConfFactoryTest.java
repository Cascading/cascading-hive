package cascading.tap.hive;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.junit.Test;

public class HiveConfFactoryTest {

  @Test
  public void testNullConfiguration() {
    HiveConf hiveConf = HiveConfFactory.getHiveConf(null);
    assertNotNull(hiveConf);
  }
  
  @Test
  public void testCopiesConfiguration() {
    Configuration configuration = new Configuration();
    configuration.set("X", "Y");
    HiveConf hiveConf = HiveConfFactory.getHiveConf(configuration);
    assertEquals("Y", hiveConf.get("X"));
  }
  
  @Test
  public void testSingleton() {
    HiveConf hiveConf1 = HiveConfFactory.getHiveConf(null);
    HiveConf hiveConf2 = HiveConfFactory.getHiveConf(null);
    assertEquals(true, hiveConf1 == hiveConf2);
  }
  
}
