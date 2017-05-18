package util;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;

import com.google.protobuf.ServiceException;

public class ConnectionFactory {
   private HBaseAdmin hbaseadmin;
   private Configuration config;
   
	
	public Configuration getConfig() {
	return config;
}



public void setConfig(Configuration config) {
	this.config = config;
}



	public HBaseAdmin getHbaseadmin() {
	return hbaseadmin;
	}



	public void setHbaseadmin(HBaseAdmin hbaseadmin) {
	this.hbaseadmin = hbaseadmin;
	}


//2181  /60000
	public void connect() {
	    config = HBaseConfiguration.create();
		config.set("hbase.master", "localhost:60000");
		config.set("hbase.zookeeper.quorum", "localhost");
		config.set("hbase.zookeeper.property.clientPort","2181");
		config.set("hbase.rpc.timeout", "120000");
		try {
			
			HBaseAdmin.checkHBaseAvailable(config);
			hbaseadmin = new HBaseAdmin( config );
			
			System.out.println("connection sucessfull");
		} catch (ServiceException | IOException e) {
			try {
				hbaseadmin.close();
			} catch (IOException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
			e.printStackTrace();
		}
		
		
	}
}
