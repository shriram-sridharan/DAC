

import java.io.FileReader;
import java.io.IOException;
import java.util.Scanner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
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
					HBaseUtils.addRecord("userdata", tokens[0], tokens[1], "", tokens[3]);
				}
			}
				
			
		}
		
	}
	
	
	public static void main(String[] args) throws Exception {
//		insertData("/home/shriram/Desktop/DAC/HBaseWrapper/src/userdata.txt");
		
		Configuration conf = HBaseConfiguration.create();
		HAuthorization authInfo = new HAuthorization("User1");
		
		
		HTableAuth objHT = null;
		try {
		 objHT = new HTableAuth(authInfo, conf, "userdata");
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		//System.out.println(authInfo.getBitVector());
		GlueZMQ.setIPAddres("localhost");
		GlueZMQ.setPortNumber("5555");
//
//		
//		
//		Put p = new Put(Bytes.toBytes("Tom"));
//		p.add(Bytes.toBytes("Age"), Bytes.toBytes(""), Bytes.toBytes("35"));
//		objHT.put(p);
		
		
		   Get g = new Get(Bytes.toBytes("Tom"));
           g.addColumn(Bytes.toBytes("Age"), Bytes.toBytes(""));
           g.addColumn(Bytes.toBytes("SSN"), Bytes.toBytes(""));

           try {
                   Result r = objHT.get(g);
//                   HBaseUtils.printResult(r);
                   
           } catch (IOException e) {
                   // TODO Auto-generated catch block
                   e.printStackTrace();
           }

		
	}	
}
