package cascading.tap.hive;

import java.io.Serializable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaHook;
import org.apache.hadoop.hive.metastore.HiveMetaHookLoader;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.RetryingMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Table;

/** Utility class for creating {@link IMetaStoreClient} instances. Assists with mocking. */
class MetaStoreClientFactory implements Serializable {

  private static final long serialVersionUID = 1L;

  MetaStoreClientFactory() {
  }

  /**
   * Create an IMetaStore client.
   *
   * @return a new IMetaStoreClient
   * @throws MetaException in case the creation fails.
   */
  IMetaStoreClient newInstance(Configuration conf) throws MetaException {
    // it is a bit unclear if it is safe to re-use these instances, so we create a
    // new one every time, to be sure
    HiveConf hiveConf = HiveConfFactory.getHiveConf(conf);

    return RetryingMetaStoreClient.getProxy(hiveConf, new HiveMetaHookLoader() {
      @Override
      public HiveMetaHook getHook(Table tbl) throws MetaException {
        return null;
      }
    }, HiveMetaStoreClient.class.getName());
  }

}
