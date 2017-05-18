package util;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.json.JSONException;
import org.json.JSONObject;



public class Parser {

	public static void main(String[] args) {

		ConnectionFactory connFactory = new ConnectionFactory();
		connFactory.connect();
		HBaseAdmin hbaseAdmin = connFactory.getHbaseadmin();
		Configuration config = connFactory.getConfig();
		try {
			HTableDescriptor[] tableDescriptor = hbaseAdmin.listTables();
		} catch (IOException e2) {
			// TODO Auto-generated catch block
			e2.printStackTrace();
		}
        
		HTableDescriptor htable = new HTableDescriptor("City");
		htable.addFamily(new HColumnDescriptor("CityDetail"));
		htable.addFamily(new HColumnDescriptor("Fussball"));
		System.out.println("Creating Table...");
		
		try {

			hbaseAdmin.createTable(htable);
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		System.out.println("Done!");

		HTable table = null;

		try {
			table = new HTable(config, "City");

			BufferedReader reader = new BufferedReader(new FileReader("/Volumes/biraj/workspace/bigdata/plz.data"));
			String s;
			int i = 0;
			HashMap<String, ArrayList<String>> items = new HashMap<String, ArrayList<String>>();
			while ( (s = reader.readLine()) != null ){
				i++;
				String jsonString = s;
				JSONObject jsonResult = new JSONObject(jsonString);
				// System.out.println(jsonResult.toString());
				String pop = jsonResult.get("pop").toString();
				String loc = jsonResult.get("loc").toString();
				String id = jsonResult.get("_id").toString();
				String city = jsonResult.get("city").toString();
				String state = jsonResult.get("state").toString();
				String key = id;
				//String key1 = city.toLowerCase().trim();

				ArrayList<String> itemsList = items.get(city);

				// if list does not exist create it
				if (itemsList == null) {
					itemsList = new ArrayList<String>();
					itemsList.add(id);
					items.put(city, itemsList);
				} else {
					// add if item is not already in list
					if (!itemsList.contains(id))
						itemsList.add(id);
				}

				Put put = new Put(Bytes.toBytes(key));
				put.add(Bytes.toBytes("CityDetail"), Bytes.toBytes("id"), Bytes.toBytes(id));
				put.add(Bytes.toBytes("CityDetail"), Bytes.toBytes("pop"), Bytes.toBytes(pop));
				put.add(Bytes.toBytes("CityDetail"), Bytes.toBytes("loc"), Bytes.toBytes(loc));
				put.add(Bytes.toBytes("CityDetail"), Bytes.toBytes("city"), Bytes.toBytes(city));
				put.add(Bytes.toBytes("CityDetail"), Bytes.toBytes("state"), Bytes.toBytes(state));

				if (city.equals("HAMBURG") || city.equals("TUMTUM")) {
					put.add(Bytes.toBytes("Fussball"), Bytes.toBytes("ja"), Bytes.toBytes("ja"));
				}
				table.put(put);
				table.flushCommits();
				System.out.println("pop:" + pop + " loc: " + loc + " id:" + id + " city: " + city + " state: " + state);

			}
			Iterator it = items.entrySet().iterator();
			while (it.hasNext()) {
				Map.Entry pair = (Map.Entry) it.next();
				//System.out.println(pair.getKey().toString() + " = " + pair.getValue().toString());
				//System.out.println(pair.getKey().toString() + " = " + pair.getValue().toString());
				String cityid = pair.getKey().toString().toLowerCase().trim();
				System.out.println("...."+ cityid + "...");
				Put put = new Put(Bytes.toBytes(cityid));
				put.add(Bytes.toBytes("CityDetail"), Bytes.toBytes("idlist"), Bytes.toBytes(pair.getValue().toString()));
				table.put(put);
				table.flushCommits();
				it.remove(); // avoids a ConcurrentModificationException
				
			}
		
		
		
	
	} catch (JSONException |

			IOException e) {
			try {
				table.close();
			} catch (IOException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
				}
				e.printStackTrace();
			}
		
	}
}

	

	

