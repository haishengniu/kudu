package com.kudu.api;

import java.util.ArrayList;
import java.util.List;

import org.apache.kudu.client.Insert;
import org.apache.kudu.client.KuduClient;
import org.apache.kudu.client.KuduScanner;
import org.apache.kudu.client.KuduSession;
import org.apache.kudu.client.KuduTable;
import org.apache.kudu.client.PartialRow;
import org.apache.kudu.client.RowResult;
import org.apache.kudu.client.RowResultIterator;

public class InsertTest {
	
	private static final String KUDU_MASTER = System.getProperty("kuduMaster", "118.192.153.114");
	
	public static void main(String[] args) {
		
		String tableName = "kudu_table_test2";
		KuduClient client = new KuduClient.KuduClientBuilder(KUDU_MASTER).build();
		try {
			
			KuduTable table = client.openTable(tableName);
			KuduSession session = client.newSession();
			Insert insert;
			PartialRow raw;
			for (int i = 31; i <= 40; i++) {
				insert = table.newInsert();	
				raw = insert.getRow();
				raw.addInt(0, i);
				raw.addString(1, "Tom" + i);
				raw.setNull("age");
				raw.addString(2, "female");
				raw.addString(4, "beijing");
				//System.out.println(insert);
				session.apply(insert);
			}
			
			
			//print(client, table);
			
			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			try {
				client.shutdown();
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
	}

	private static void print(KuduClient client, KuduTable table) throws Exception {
		List<String> list = new ArrayList<>();
		list.add("id");
		list.add("name");
		KuduScanner scanner = client.newScannerBuilder(table).setProjectedColumnNames(list).build();
		while (scanner.hasMoreRows()) {
		    RowResultIterator results = scanner.nextRows();
		    while (results.hasNext()) {
		      RowResult result = results.next();
		      System.out.println(result.getLong(0) + " " + result.getString(1));
		    }
		}
	}
}