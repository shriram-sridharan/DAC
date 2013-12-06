package utils;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.zeromq.ZMQ;

public class HTableAuth extends HTable {
	private HAuthorization objAuth;
	private String tableName;
	
	public HTableAuth(HAuthorization authInfo, Configuration c, String tableName) throws IOException {
		super(c, tableName);
		this.tableName = tableName;
		objAuth = authInfo;
	}
	
	private boolean isGetAuthorized(String key, String columnFamily) {
		//GET;tablename;key;cf;columnname;AuthBitVector
		
		StringBuffer objSB = new StringBuffer();
		objSB.append("GET;").append(tableName).append(";").append(key).append(";").append(columnFamily).append(";").append(";")
		.append(objAuth.getBitVector());
		System.out.println(objSB.toString());
		boolean retValue = GlueZMQ.isAuthorized(objSB.toString());
		if(retValue == true) {
			System.out.println("Get is authorized: ");
			return true;
		}
		return false;
	}
	private boolean isPutAuthorized(String key, String columnFamily) {
		//PUT;tablename;key;cf;columnname;AuthBitVector
		
		StringBuffer objSB = new StringBuffer();
		objSB.append("PUT;").append(tableName).append(";").append(key).append(";").append(columnFamily).append(";").append(";")
		.append(objAuth.getBitVector());
		System.out.println(objSB.toString());
		boolean retValue = GlueZMQ.isAuthorized(objSB.toString());
		if(retValue == true) {
			System.out.println("Put is authorized: ");
			return true;
		}
		return false;
	}
	// override required methods
	public Result get(Get get) throws IOException {
		// 2 cases 
		// case 1: If the column families are added
		// if all the columns are allowed: fetch the row, else return null;
		  
		
		// case 2: they are not.
			// this has to be handled.
		
		
		boolean authFlag = true;
		if(get.hasFamilies()) {
			Map<byte[],NavigableSet<byte[]>> objFM = get.getFamilyMap();
			Set<byte[]> keySet = objFM.keySet();
			
			for(byte[] by : keySet) {
			
				NavigableSet<byte[]> ns1 = objFM.get(by);
				for(Iterator<byte[]> iter = ns1.iterator(); iter.hasNext();) {
					byte[] b = iter.next();
					String rowKey = Bytes.toString(get.getRow());
					String columnFamily = Bytes.toString(b);
					if(isGetAuthorized(rowKey, columnFamily) == false) {
						authFlag = false;
						break;
					}
				}

			}

		}
		if(authFlag == false)
			return null;
		else
			return super.get(get);
	}
	
	public void put(Put put) throws IOException {
		
		boolean authFlag = true;
		Map<byte[], List<KeyValue>> objM =  put.getFamilyMap();
		String rowKey = Bytes.toString(put.getRow());
		assert(objM.keySet().size() == 1);
		for(byte[] b : objM.keySet()) {
			String columnFamily = Bytes.toString(b);
			if(isPutAuthorized(rowKey, columnFamily)) {
				super.put(put);
			} else {
				//System.err.println("Put with current columnfamilies is not authorized");
			}
		}
		
	}
	

}
