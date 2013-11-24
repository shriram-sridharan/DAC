

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;

import utils.HBaseUtils;

public class Test {
	public static void main(String[] args) {
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

}
