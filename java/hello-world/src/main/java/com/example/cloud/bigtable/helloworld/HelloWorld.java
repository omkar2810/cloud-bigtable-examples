/**
 * Copyright 2016 Google Inc. All Rights Reserved.
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.example.cloud.bigtable.helloworld;
// [START bigtable_hw_imports]
import com.google.cloud.bigtable.hbase.BigtableConfiguration;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;

import java.io.BufferedReader;
import java.io.FileReader;

import java.io.IOException;
import java.io.FileNotFoundException;
// import java.io.*;
import java.util.*;
// import javafx.util.Pair;
// import java.nio.charset.StandardCharsets;
// // import java.nio.file.*;

// import java.io.BufferedReader;
// import java.io.FileReader;
// [END bigtable_hw_imports]

/**
 * A minimal application that connects to Cloud Bigtable using the native HBase API and performs
 * some basic operations.
 */

class PrintResults{

    public static void print(String fn, int[] arr)
    {
        System.out.println(fn);

        for(int i=0;i<arr.length;i++)
        {
            System.out.print(arr[i]+" ");
        }

        System.out.println();

        return;
    }

    public static void print(String fn, int a)
    {
        System.out.println(fn);
        System.out.println(a);

        return;
    }

}

class Pair {

    public int first;
    public int second;

    public Pair(int first, int second)
    {
        this.first = first;
        this.second = second;
    }

    public boolean compare(Pair one, Pair two)
    {
        return one.first >= two.second;
    }
    
}

class PairComparator implements Comparator<Pair>{ 
              
            // Overriding compare()method of Comparator  
                        // for descending order of cgpa 
            public int compare(Pair s1, Pair s2) { 
                if (s1.first <= s2.first) 
                    return 1; 
                else  
                    return -1;                
                } 
        } 

 public class HelloWorld {

  // Refer to table metadata names by byte array in the HBase API
  private static final byte[] TABLE_NAME = Bytes.toBytes("Team1314_Table");
  private static final byte[] COLUMN_FAMILY_NAME = Bytes.toBytes("cf1");
  private static final byte[] COLUMN_NAME = Bytes.toBytes("greeting");

  private static final byte[] COLUMN_1 = Bytes.toBytes("userID");
  private static final byte[] COLUMN_2 = Bytes.toBytes("itemID");
  private static final byte[] COLUMN_3 = Bytes.toBytes("valueCounts");

//   private static List<String> userID = new ArrayList<String>();
//   private static List<String> itemID = new ArrayList<String>();
//   private static List<String> valueCounts = new ArrayList<String>();

  private static String projectId;
  private static String instanceId;
  // Write some friendly greetings to Cloud Bigtable
  

  private static int[] top(int userID, int K) {
      int[] result = new int[K];
    //   System.out.println("top("+userID+","+K+")");
       try (Connection connection = BigtableConfiguration.connect(projectId, instanceId)) {

      // The admin API lets us create, manage and delete tables
      Admin admin = connection.getAdmin();
      // [END bigtable_hw_connect]

      try {
        // [START bigtable_hw_create_table]
        // Create a table with a single column family
        HTableDescriptor descriptor = new HTableDescriptor(TableName.valueOf(TABLE_NAME));
        Table table = connection.getTable(TableName.valueOf(TABLE_NAME));

        SingleColumnValueFilter filter = new SingleColumnValueFilter(COLUMN_FAMILY_NAME,
        COLUMN_1,
        CompareOp.EQUAL,
        Bytes.toBytes(userID)
        );
        
        Scan scan = new Scan();
        scan.setFilter(filter);
        
        PriorityQueue<Pair> pQueue = new PriorityQueue<Pair>(K+2, new PairComparator());
        // print("Scan for all greetings:");
        ResultScanner scanner = table.getScanner(scan);
        for (Result row : scanner) {
        
          byte[] item = row.getValue(COLUMN_FAMILY_NAME, COLUMN_2);
          byte[] value = row.getValue(COLUMN_FAMILY_NAME, COLUMN_3);
          Pair temp = new Pair(Integer.parseInt(Bytes.toString(value)),Integer.parseInt(Bytes.toString(item)));
        //   print(Integer.toString(temp.first)+" "+ Integer.toString(temp.second));
        //   Pair temp = new Pair(1,2);
          pQueue.add(temp);
        }

        for(int i=0;i<K;i++)
        {
            result[i] = pQueue.poll().second; 
            // System.out.println(Integer.toString(result[i]));
            
        }
        // [END bigtable_hw_delete_table]
      } catch (IOException e) {
        if (admin.tableExists(TableName.valueOf(TABLE_NAME))) {
        //   print("Cleaning up table");
        }
        throw e;
      }
    } catch (IOException e) {
      System.err.println("Exception while running HelloWorld: " + e.getMessage());
      e.printStackTrace();

    }
      return result;
  }

private static int interested(int itemID)  {
    int result = 0;
    // System.out.println("interested("+itemID+")");
       try (Connection connection = BigtableConfiguration.connect(projectId, instanceId)) {

      // The admin API lets us create, manage and delete tables
      Admin admin = connection.getAdmin();
      // [END bigtable_hw_connect]

      try {
        // [START bigtable_hw_create_table]
        // Create a table with a single column family
        HTableDescriptor descriptor = new HTableDescriptor(TableName.valueOf(TABLE_NAME));
        Table table = connection.getTable(TableName.valueOf(TABLE_NAME));

        SingleColumnValueFilter filter = new SingleColumnValueFilter(COLUMN_FAMILY_NAME,
        COLUMN_2,
        CompareOp.EQUAL,
        Bytes.toBytes(itemID)
        );
        
        Scan scan = new Scan();
        scan.setFilter(filter);
        
        ResultScanner scanner = table.getScanner(scan);
        for (Result row : scanner) {
            result += 1;
        }

        // [END bigtable_hw_delete_table]
      } catch (IOException e) {
        if (admin.tableExists(TableName.valueOf(TABLE_NAME))) {
          print("Cleaning up table");
        }
        throw e;
      }
    } catch (IOException e) {
      System.err.println("Exception while running HelloWorld: " + e.getMessage());
      e.printStackTrace();

    }
        // System.out.println(result);
      return result;
  }

private static int view_count(int itemID)  {
    int result = 0;
    // System.out.println("view_count("+itemID+")");
       try (Connection connection = BigtableConfiguration.connect(projectId, instanceId)) {

      // The admin API lets us create, manage and delete tables
      Admin admin = connection.getAdmin();
      // [END bigtable_hw_connect]

      try {
        // [START bigtable_hw_create_table]
        // Create a table with a single column family
        HTableDescriptor descriptor = new HTableDescriptor(TableName.valueOf(TABLE_NAME));
        Table table = connection.getTable(TableName.valueOf(TABLE_NAME));

        SingleColumnValueFilter filter = new SingleColumnValueFilter(COLUMN_FAMILY_NAME,
        COLUMN_2,
        CompareOp.EQUAL,
        Bytes.toBytes(itemID)
        );
        
        Scan scan = new Scan();
        scan.setFilter(filter);
        
        ResultScanner scanner = table.getScanner(scan);
        for (Result row : scanner) {
            byte[] value = row.getValue(COLUMN_FAMILY_NAME, COLUMN_3);
            result += Integer.parseInt(Bytes.toString(value));
        }

        // [END bigtable_hw_delete_table]
      } catch (IOException e) {
        if (admin.tableExists(TableName.valueOf(TABLE_NAME))) {
          print("Cleaning up table");
        }
        throw e;
      }
    } catch (IOException e) {
      System.err.println("Exception while running HelloWorld: " + e.getMessage());
      e.printStackTrace();

    }
        // System.out.println(result);
      return result;
  }

    private static int popular()  {
    // System.out.println("popular()");
       Pair result = new Pair(-1,-1);
       try (Connection connection = BigtableConfiguration.connect(projectId, instanceId)) {

      // The admin API lets us create, manage and delete tables
      Admin admin = connection.getAdmin();
      // [END bigtable_hw_connect]

      try {
        // [START bigtable_hw_create_table]
        // Create a table with a single column family
        HTableDescriptor descriptor = new HTableDescriptor(TableName.valueOf(TABLE_NAME));
        Table table = connection.getTable(TableName.valueOf(TABLE_NAME));

        HashMap<Integer,Integer> map= new HashMap<Integer,Integer>();
        Scan scan = new Scan();
        // scan.setFilter(filter);
        
        ResultScanner scanner = table.getScanner(scan);
        for (Result row : scanner) {

            Integer item = Integer.parseInt(Bytes.toString(row.getValue(COLUMN_FAMILY_NAME, COLUMN_2)));
           
            if(map.get(item)!=null)
            {
                map.put(item,1+map.get(item));
            }
            else
            {
                map.put(item,1);
            }
            
            if(map.get(item)>result.second)
            {
                result.first = item;
                result.second = map.get(item);
            }
        
        }

        // [END bigtable_hw_delete_table]
      } catch (IOException e) {
        if (admin.tableExists(TableName.valueOf(TABLE_NAME))) {
          print("Cleaning up table");
        }
        throw e;
      }
    } catch (IOException e) {
      System.err.println("Exception while running HelloWorld: " + e.getMessage());
      e.printStackTrace();

    }
    //  System.out.println(result.first);
      return result.first;
  }

    private static int[] top_interested(int itemID, int K)  {
    // System.out.println("top_interested("+itemID+","+K+")");
       ArrayList<Integer> result = new ArrayList<Integer>();
       try (Connection connection = BigtableConfiguration.connect(projectId, instanceId)) {

      // The admin API lets us create, manage and delete tables
      Admin admin = connection.getAdmin();
      // [END bigtable_hw_connect]

      try {
        // [START bigtable_hw_create_table]
        // Create a table with a single column family
        HTableDescriptor descriptor = new HTableDescriptor(TableName.valueOf(TABLE_NAME));
        Table table = connection.getTable(TableName.valueOf(TABLE_NAME));

        Scan scan = new Scan();
        SingleColumnValueFilter filter = new SingleColumnValueFilter(COLUMN_FAMILY_NAME,
        COLUMN_2,
        CompareOp.EQUAL,
        Bytes.toBytes(itemID)
        );
        scan.setFilter(filter);
        
        ResultScanner scanner = table.getScanner(scan);
        for (Result row : scanner) {

            int userID = Bytes.toInt(row.getValue(COLUMN_FAMILY_NAME, COLUMN_1));
            // System.out.println("userID " + userID);
            int[] topK = top(userID, K);
            for(int i = 0 ; i < topK.length ; ++i) {
                // System.out.println("topKVal " + topK[i]);
                result.add(topK[i]);
            }
        }

        // [END bigtable_hw_delete_table]
      } catch (IOException e) {
        if (admin.tableExists(TableName.valueOf(TABLE_NAME))) {
          print("Cleaning up table");
        }
        throw e;
      }
    } catch (IOException e) {
      System.err.println("Exception while running HelloWorld: " + e.getMessage());
      e.printStackTrace();

    }
    int[] ret = new int[result.size()];
    for (int i=0; i < ret.length; i++) {
        ret[i] = result.get(i).intValue();
        // System.out.println(ret[i]);
    }
    return ret;
  }

  /** Connects to Cloud Bigtable, runs some basic operations and prints the results. */
  private static void createTable(String projectId, String instanceId, String filePath) {
    System.out.println("createTable("+projectId+","+instanceId+")");
    
    // [START bigtable_hw_connect]
    // Create the Bigtable connection, use try-with-resources to make sure it gets closed
    try (Connection connection = BigtableConfiguration.connect(projectId, instanceId)) {

      // The admin API lets us create, manage and delete tables
      Admin admin = connection.getAdmin();
      // [END bigtable_hw_connect]

      try {
        
        if(admin.tableExists(TableName.valueOf(TABLE_NAME)))
        {
            admin.disableTable(TableName.valueOf(TABLE_NAME));
            admin.deleteTable(TableName.valueOf(TABLE_NAME));
        }

        HTableDescriptor descriptor = new HTableDescriptor(TableName.valueOf(TABLE_NAME));
        descriptor.addFamily(new HColumnDescriptor(COLUMN_FAMILY_NAME));
        System.out.println("Create table " + descriptor.getNameAsString());
        admin.createTable(descriptor);
        Table table = connection.getTable(TableName.valueOf(TABLE_NAME));

        List<Put> puts = new ArrayList<Put>();
        try
        {
            String splitBy = ",";
            BufferedReader br = new BufferedReader(new FileReader("data.csv"));
            try{
            int cnt = 0;
            int batch = 10e6;
            String line = br.readLine();
            while((line = br.readLine()) != null){

                if(cnt%batch==0 && cnt!=0)
                {
                    table.put(puts);
                    puts.clear();
                }
                String[] b = line.split(splitBy);
                String userID = b[0];
                String itemID = b[1];
                String valueCounts = b[2];

                String rowKey = userID + "-" + itemID ;
                Put put = new Put(Bytes.toBytes(rowKey));
                put.addColumn(COLUMN_FAMILY_NAME, COLUMN_1, Bytes.toBytes(Integer.parseInt(userID)));
                put.addColumn(COLUMN_FAMILY_NAME, COLUMN_2, Bytes.toBytes(Integer.parseInt(itemID)));
                put.addColumn(COLUMN_FAMILY_NAME, COLUMN_3, Bytes.toBytes(Integer.parseInt(valueCounts)));
                puts.add(put);
                cnt += 1;
                // System.out.println(userID+" "+itemID+" "+valueCounts);
            }
            table.put(puts);
            br.close();
            }
            catch(IOException e)
            {
                System.out.println("Cant read file");
            }

        }  
        catch (FileNotFoundException e)
        {
            System.out.println("File Not Found");
        }


      } catch (IOException e) {
        if (admin.tableExists(TableName.valueOf(TABLE_NAME))) {
          print("Cleaning up table");
        //   admin.disableTable(TableName.valueOf(TABLE_NAME));
        //   admin.deleteTable(TableName.valueOf(TABLE_NAME));
        }
        throw e;
      }
    } catch (IOException e) {
      System.err.println("Exception while running HelloWorld: " + e.getMessage());
      e.printStackTrace();

      System.exit(1);
    }

    // System.exit(0);
  }

  private static void print(String msg) {
    System.out.println("HelloWorld: " + msg);
  }

  private static int int2str(String s)
  {
      if(s==null)
        return -1;
      else
        return Integer.parseInt(s);
  }

  public static void main(String[] args) {
    // Consult system properties to get project/instance

    projectId = requiredProperty("bigtable.projectID");
    instanceId = requiredProperty("bigtable.instanceID");

    String filePath = optionalProperty("filePath");
    String query = optionalProperty("query");

    if(filePath != null){
        createTable(projectId, instanceId, filePath);
    }
    int userId = int2str(optionalProperty("userId"));
    int K = int2str(optionalProperty("K"));
    int itemId = int2str(optionalProperty("itemId"));
    System.out.println(itemId);
    if(query.equals("top"))
    {  
        PrintResults.print("top",top(userId,K));
    }    
    else if(query.equals("interested")) 
    {
        PrintResults.print("interested",interested(itemId));
    }
    else if(query.equals("popular"))
    {
    PrintResults.print("popular",popular());
    }
    else if(query.equals("view_count"))
    {
    PrintResults.print("view_count",view_count(itemId));
    }
    else if(query.equals("top_interested"))
    {
        PrintResults.print("top_interested",top_interested(itemId, K));
    }
    System.exit(0);

  }

  private static String requiredProperty(String prop) {
    String value = System.getProperty(prop);
    if (value == null) {
      throw new IllegalArgumentException("Missing required system property: " + prop);
    }
    return value;
  }

  private static String optionalProperty(String prop) {
    String value = System.getProperty(prop);
    return value;
  }


}
