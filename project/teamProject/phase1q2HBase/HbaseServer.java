import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;

/**
 * @author cmucchackers
 *
 */
public class HbaseServer {
	private static final int TABLE_CLIENT_NUMBER = 50000;
	private static HTable[] htable = new HTable[TABLE_CLIENT_NUMBER];
	public static Configuration configuration = HBaseConfiguration.create();
	static {
		try {
			configuration.addResource("/home/hadoop/hbase/conf/hbase-site.xml");
			configuration.set("fs.hdfs.impl", "emr.hbase.fs.BlockableFileSystem");
			configuration.set("hbase.regionserver.handler.count", "100");
			configuration.set("hbase.zookeeper.quorum", "localhost");
			configuration.set("hbase.rootdir", "hdfs://localhost:9000/hbase");
			configuration.set("hbase.cluster.distributed", "true");
			configuration.set("hbase.tmp.dir", "/mnt/var/lib/hbase/tmp-data");
		} catch (Exception e) {
			e.printStackTrace();
		}
		try {
			for (int i = 0; i < TABLE_CLIENT_NUMBER; i++) {
				htable[i] = new HTable(configuration, "twitterdata");
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static String selectRowKey(String rowKey, int tableNum)
			throws IOException {
		Get g = new Get(rowKey.getBytes());
		Result rs = htable[tableNum].get(g);
		String result = "";
		for (KeyValue kv : rs.raw()) {
			if (new String(kv.getQualifier()).equals("text")) {
				result += new String(kv.getValue()).replace("\\n", "\n").replace("\\t", "\t").replace("\\r", "\r").replace("\\\\", "\\") + "\n";
			}
		}
		return result;
	}
}
