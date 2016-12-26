package com.dreams.hadoop;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * InsertData
 * @author selva
 *
 */
public class InsertData {

   public static void main(String[] args) throws IOException {

      // Instantiating Configuration class
      Configuration config = HBaseConfiguration.create();

      HTable hTable = new HTable(config, "employee");

      Put p = new Put(Bytes.toBytes("row1")); 

      p.add(Bytes.toBytes("official"),
      Bytes.toBytes("designation"),Bytes.toBytes("project lead"));

      p.add(Bytes.toBytes("official"),
      Bytes.toBytes("salary"),Bytes.toBytes("50000"));

      p.add(Bytes.toBytes("personal"),Bytes.toBytes("name"),
      Bytes.toBytes("selvakumar"));

      p.add(Bytes.toBytes("personal"),Bytes.toBytes("city"),
      Bytes.toBytes("bangalore"));
      
      hTable.put(p);
      System.out.println("data inserted row1");
      
      Put p1 = new Put(Bytes.toBytes("row2")); 

      p1.add(Bytes.toBytes("official"),
      Bytes.toBytes("designation"),Bytes.toBytes("Team Lead"));

      p1.add(Bytes.toBytes("official"),
      Bytes.toBytes("salary"),Bytes.toBytes("70000"));

      p1.add(Bytes.toBytes("personal"),Bytes.toBytes("name"),
      Bytes.toBytes("selva"));

      p1.add(Bytes.toBytes("personal"),Bytes.toBytes("city"),
      Bytes.toBytes("bangalore"));
      
      hTable.put(p1);
      System.out.println("data inserted row2");
      
      // closing HTable
      hTable.close();
   }
}