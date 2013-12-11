

import java.io.FileReader;
import java.io.IOException;
import java.util.Scanner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import utils.GlueZMQ;
import utils.HAuthorization;
import utils.HBaseUtils;
import utils.HTableAuth;




public class Test {

/*	public static void main(String[] args) {
		String[] families = {"Name", "Role"};
		try {
			HBaseUtils.creatTable("AuthorizationInfo", families);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		try {
			HBaseUtils.addRecord("AuthorizationInfo", "User1", "Name", "", "Tom");
			HBaseUtils.addRecord("AuthorizationInfo", "User1", "Role", "1", "Admin");
			HBaseUtils.addRecord("AuthorizationInfo", "User1", "Role", "2", "System");
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		ResultScanner rs = HBaseUtils.getAllRecord("AuthorizationInfo");
		for(Result r:rs) {
			for(KeyValue kv : r.raw()) {
                System.out.print(new String(kv.getRow()) + " ");
                System.out.print(new String(kv.getFamily()) + ":");
                System.out.print(new String(kv.getQualifier()) + " ");
                System.out.print(kv.getTimestamp() + " ");
                System.out.println(new String(kv.getValue()));

			}
		}
		
	}
*/
	public static void insertData(String filename) throws Exception {
		Scanner objSC = new Scanner(new FileReader(filename));
		boolean first = true;
		String line;
		while(objSC.hasNext()) {
			line = objSC.nextLine();
			String[] tokens = line.split(",");
			
			if(tokens.length != 4)
				continue;
			else {
				if(first == true){
					HBaseUtils.creatTable("userdata", tokens);
					first = false;
				} else {
					HBaseUtils.addRecord("userdata", tokens[0], tokens[1], tokens[2], tokens[3]);
				}
			}
				
			
		}
		
	}
	
	
	public static void main(String[] args) throws Exception {
		//insertData("/scratch/pradap/courses/cs739/data/userdata.txt");
		args = new String[3];
		args[0] = new String("User1");
		args[1] = new String("GET");
		args[2] = new String("System");
		Configuration conf = HBaseConfiguration.create();
		
		if("UPGRADE".equals(args[1])){
			HTable hTable = new HTable(conf, "AuthorizationInfo");
			System.out.println("\n\n===============================================================");
			System.out.println("[UPGRADING] " + args[0] + " to have " + args[2] + " previleges\n");
			Put p = new Put(Bytes.toBytes(args[0]));
			p.add(Bytes.toBytes("Role"), Bytes.toBytes("1"), Bytes.toBytes(args[2]));
			hTable.put(p);
			return;
		}
		
		HAuthorization authInfo = new HAuthorization(args[0]);

		HTableAuth objHT = null;
		try {
			objHT = new HTableAuth(authInfo, conf, "userdata");
		} catch (IOException e) {
			e.printStackTrace();
		}


		GlueZMQ.setIPAddres("127.0.0.1");
		GlueZMQ.setPortNumber("5555");

		if ("GET".equals(args[1])) {
			System.out.println("[AUTHORIZING AND GETTING RESULTS] Querying RowID with Name1..Name5");
			System.out.println("\nRowID\tColumnFamily:Column\tTimeStamp\tValue\n");
			for(int i = 1; i <= 5; i++) {
				Get g = new Get(Bytes.toBytes("Name" + i));
				g.addColumn(Bytes.toBytes("Personal"), Bytes.toBytes("Age"));
				g.addColumn(Bytes.toBytes("Personal"), Bytes.toBytes("SSN"));
				g.addColumn(Bytes.toBytes("Personal"), Bytes.toBytes("MaritalStatus"));
	
				try {
					Result r = objHT.get(g);
					if (r != null)
						HBaseUtils.printResult(r);
				} catch (IOException e) {
					e.printStackTrace();
				}
				System.out.println();
			}
		} else {
			Put p = new Put(Bytes.toBytes("Tom"));
			p.add(Bytes.toBytes("Age"), Bytes.toBytes(""), Bytes.toBytes("35"));
			objHT.put(p);
		}



		
/*		
		Put p = new Put(Bytes.toBytes("Tom"));
		p.add(Bytes.toBytes("Age"), Bytes.toBytes(""), Bytes.toBytes("35"));
		objHT.put(p);*/
		/*
		
		   Get g = new Get(Bytes.toBytes("user995698996184959679"));
           g.addColumn(Bytes.toBytes("family"), Bytes.toBytes("field0"));
           g.addColumn(Bytes.toBytes("family"), Bytes.toBytes("field1"));
           g.addColumn(Bytes.toBytes("family"), Bytes.toBytes("field2"));
           
           //g.addColumn(Bytes.toBytes("SSN"), Bytes.toBytes(""));

           try {
                   Result r = objHT.get(g);
                   if(r != null)
                	   HBaseUtils.printResult(r);
                   
           } catch (IOException e) {
                   // TODO Auto-generated catch block
                   e.printStackTrace();
           }

		*/

	}	
}
