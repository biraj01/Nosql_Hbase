package query;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.PageFilter;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;
import com.google.protobuf.ServiceException;

import util.ConnectionFactory;

public class Query {

	public static void main(String[] args) {

		boolean running = true;
		Query query = new Query();

		while (running) {
			String s = "";
			BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
			System.err.println("Befehl <Parameter> Gültige befehl");
			System.err.println("quit<No Param>");
			System.err.println("findcity <plz>");
			System.err.println("findstate <plz>");
			System.err.println("findfussball <plz>");
			System.err.println("findstateandcity <plz>");
			System.err.println("findplz <city>");

			try {
				s = br.readLine().toLowerCase();
				// TODO Auto-generated catch block

				if (s.matches("quit")) {
					ConnectionFactory connFactory = new ConnectionFactory();
					connFactory.connect();

					Admin admin = connFactory.getHbaseadmin();
					admin.close();
					running = false;
				} else if (s.matches("findcity .*")) {
					String id = s.replaceFirst("findcity ", "");

					System.out.println(query.getCity(id));

				} else if (s.matches("findstate .*")) {
					String id = s.replaceFirst("findstate ", "");
					System.out.println(query.getState(id));
				} else if (s.matches("findfussball .*")) {
					String id = s.replaceFirst("findfussball ", "");
					System.out.println(query.getFussball(id));
				} else if (s.matches("findstateandcity .*")) {
					String id = s.replaceFirst("findstateandcity ", "");
					System.out.println(query.getStateandCity(id));
				} else if (s.matches("findplz .*")) {
					String city = s.replaceFirst("findplz ", "");
					System.out.println(query.getPlz(city));
				} else {
					System.err.println("Illegal Parameter Gütige eingabe eingeben");
				}

			} catch (ServiceException | IOException e) {

				e.printStackTrace();
			}
		}

	}

	public String getCity(String id) throws IOException {
		ConnectionFactory connFactory = new ConnectionFactory();
		connFactory.connect();
		Configuration config = connFactory.getConfig();
		Admin admin = connFactory.getHbaseadmin();
		HTable table = new HTable(config, "City");
		Get get = new Get(Bytes.toBytes(id));
		get.addFamily(Bytes.toBytes("CityDetail"));
		Result result = table.get(get);
		byte[] value = result.getValue(Bytes.toBytes("CityDetail"), Bytes.toBytes("city"));
		String city = Bytes.toString(value);
		table.close();
		return city;

	}

	public String getFussball(String id)
			throws MasterNotRunningException, ZooKeeperConnectionException, ServiceException, IOException {
		ConnectionFactory connFactory = new ConnectionFactory();
		connFactory.connect();
		Configuration config = connFactory.getConfig();
		Admin admin = connFactory.getHbaseadmin();
		HTable table = new HTable(config, "City");
		Get get = new Get(Bytes.toBytes(id));
		get.addFamily(Bytes.toBytes("Fussball"));
		Result result = table.get(get);
		byte[] value = result.getValue(Bytes.toBytes("Fussball"), Bytes.toBytes("ja"));
		String fussball = Bytes.toString(value);
		table.close();
		return fussball;

	}

	public String getState(String id)
			throws MasterNotRunningException, ZooKeeperConnectionException, ServiceException, IOException {

		ConnectionFactory connFactory = new ConnectionFactory();
		connFactory.connect();
		Configuration config = connFactory.getConfig();
		Admin admin = connFactory.getHbaseadmin();
		HTable table = new HTable(config, "City");
		Get get = new Get(Bytes.toBytes(id));
		get.addFamily(Bytes.toBytes("CityDetail"));
		Result result = table.get(get);
		byte[] value = result.getValue(Bytes.toBytes("CityDetail"), Bytes.toBytes("state"));
		String state = Bytes.toString(value);
		table.close();
		return state;
	}

	public String getPlz(String city) throws IOException {
		ConnectionFactory connFactory = new ConnectionFactory();
		connFactory.connect();
		Configuration config = connFactory.getConfig();
		Admin admin = connFactory.getHbaseadmin();
		HTable table = new HTable(config, "City");
		Get get = new Get(Bytes.toBytes(city));
		get.addFamily(Bytes.toBytes("CityDetail"));
		Result result = table.get(get);
		byte[] value = result.getValue(Bytes.toBytes("CityDetail"), Bytes.toBytes("idlist"));
		String res = Bytes.toString(value);
		table.close();
		return res;

//		ConnectionFactory connFactory = new ConnectionFactory();
//		connFactory.connect();
//		Configuration config = connFactory.getConfig();
//		Admin admin = connFactory.getHbaseadmin();
//		HTable table = table = new HTable(config, "CityInfo");
//		Scan scan = new Scan();
//
//		// Scanning the required columns
//		scan.addColumn(Bytes.toBytes("CityDetail"),Bytes.toBytes("city"));
//		// Getting the scan result
//		ResultScanner scanner = null;
//			scanner = table.getScanner(scan);
//			System.out.println(scanner.toString());
//			// Reading values from scan result
//			for (Result result = scanner.next(); result != null; result = scanner
//					.next()) {
//				String cityName = Bytes.toString(result.getValue(
//						Bytes.toBytes("CityDetail"),
//						Bytes.toBytes("city")));
//				System.out.println("City Found: " + Bytes.toInt(result.getRow()));
//					System.out.println((String.valueOf(Bytes.toInt(result.getRow()))));
//				
//			}
//		return "";
	}

	

	public String getStateandCity(String id) {

		return "";

	}

}
