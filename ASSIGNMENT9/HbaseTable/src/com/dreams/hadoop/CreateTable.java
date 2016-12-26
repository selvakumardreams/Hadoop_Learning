package com.dreams.hadoop;

import java.io.IOException;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.TableName;

import org.apache.hadoop.conf.Configuration;

/**
 * CreateTable
 * @author Selva
 *
 */
public class CreateTable {

	public static void main(String[] args) throws IOException {

		// Instantiating configuration class
		Configuration con = HBaseConfiguration.create();

		// Instantiating HbaseAdmin class
		HBaseAdmin admin = new HBaseAdmin(con);

		// Instantiating table descriptor class
		HTableDescriptor tableDescriptor = new
				HTableDescriptor(TableName.valueOf("employee"));

		// Adding column families to table descriptor
		tableDescriptor.addFamily(new HColumnDescriptor("official"));
		tableDescriptor.addFamily(new HColumnDescriptor("personal"));

		// Execute the table through admin
		admin.createTable(tableDescriptor);
		System.out.println(" Table created ");
	}
}