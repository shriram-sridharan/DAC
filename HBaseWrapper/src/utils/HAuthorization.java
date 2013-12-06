package utils;

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

public class HAuthorization {
	private String userName;
	private String bitVector;
	private String[] authValues;
	/**
	 * @param userName the userName to set
	 */
	public HAuthorization() {

	}
public HAuthorization(String userName) {
		setUserName(userName);
		setAuthValues(userName);
		setBitVector();
	}

	
	public void setUserName(String userName) {
		this.userName = userName;

	}
	/**
	 * @return the userName
	 */
	public String getUserName() {
		return userName;
	}
	/**
	 * @param bitVector the bitVector to set
	 */
	private void setBitVector() {
		// 5 different roles
		// bit 0: Admin, 1: system, 2: poweruser, 3: user, 4: guest
		StringBuffer sAuth = new StringBuffer("0000000000");
		for(int i = 0; i < authValues.length; i ++) {
			String role = authValues[i].toLowerCase();
			if(role.equalsIgnoreCase("admin")) {
				sAuth.replace(0, 0, "1");
				
			} else if(role.equalsIgnoreCase("system")) {
				sAuth.replace(1, 1, "1");
			} else if(role.equalsIgnoreCase("poweruser")) {
				sAuth.replace(2, 2, "1");
			} else if(role.equalsIgnoreCase("user")) {
				sAuth.replace(3, 3, "1");
			} else if(role.equalsIgnoreCase("guest")) {
				sAuth.replace(4, 4, "1");
			}
		}
		this.bitVector = sAuth.toString();
	}
	private void setAuthValues(String userName) {
		
		try {
			Result r = HBaseUtils.getOneRecord ("AuthorizationInfo", userName);
			this.authValues = new String[r.list().size()-1];
			int i = 0;
			boolean first = true;
			for(KeyValue kv1 : r.raw()) {
				if(first == false) {
				String s = Bytes.toString(kv1.getValue());
				//System.out.println(s);
				this.authValues[i ++] =s;
				//System.out.println("fdsfs" + " " +this.authValues[i-1]);
				
				} else {
					first = false;
				}
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}
	public void setBitVector(String bitVector) {
		this.bitVector = bitVector;
	}
	/**
	 * @return the bitVector
	 */
	public String getBitVector() {
		return bitVector;
	}
	/**
	 * @param authValues the authValues to set
	 */
	public void setAuthValues(String[] authValues) {
		this.authValues = authValues.clone();
	}
	/**
	 * @return the authValues
	 */
	public String[] getAuthValues() {
		return authValues;
	}


}
