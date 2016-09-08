package com.flumesink.kudu.test;

import org.apache.flume.Context;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.conf.Configurable;
import org.apache.flume.sink.AbstractSink;
import org.apache.kudu.client.Insert;
import org.apache.kudu.client.KuduClient;
import org.apache.kudu.client.KuduException;
import org.apache.kudu.client.KuduSession;
import org.apache.kudu.client.KuduTable;
import org.apache.kudu.client.PartialRow;

public class KuduSink extends AbstractSink implements Configurable{
	String masterAddress;
	String tableName;
	@Override
	public Status process() throws EventDeliveryException {
		// TODO Auto-generated method stub
		
		KuduClient client = new KuduClient.KuduClientBuilder("118.192.153.114").build();
		try {
			KuduTable table = client.openTable("my_first_table");
			KuduSession session = client.newSession();
			Insert insert = table.newInsert();
			PartialRow row = insert.getRow();
			row.addInt(0, 1);
			row.addString(1, "hello");
			session.apply(insert);
		} catch (KuduException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
		return null;
	}

	@Override
	public void configure(Context arg0) {
		// TODO Auto-generated method stub
		masterAddress = "118.192.153.114";
		tableName = "my_first_table";
		
	}
	
	
	
}
