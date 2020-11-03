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
  private static final byte[] TABLE_NAME = Bytes.toBytes("Hello-Bigtable");
  private static final byte[] COLUMN_FAMILY_NAME = Bytes.toBytes("cf1");
  private static final byte[] COLUMN_NAME = Bytes.toBytes("greeting");

  private static final byte[] COLUMN_1 = Bytes.toBytes("userID");
  private static final byte[] COLUMN_2 = Bytes.toBytes("itemID");
  private static final byte[] COLUMN_3 = Bytes.toBytes("valueCounts");

  private static List<String> userID = new ArrayList<String>();
  private static List<String> itemID = new ArrayList<String>();
  private static List<String> valueCounts = new ArrayList<String>();

  private static String projectId;
  private static String instanceId;
  // Write some friendly greetings to Cloud Bigtable
  private static final String[] GREETINGS = {
    "Hello World!", "Hello Cloud Bigtable!", "Hello HBase!"
  };

  private static int[] top(String userID, int K) {
      int[] result = new int[K+2];
    print(Integer.toString(K));
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
        print("Scan for all greetings:");
        ResultScanner scanner = table.getScanner(scan);
        for (Result row : scanner) {
        
          byte[] item = row.getValue(COLUMN_FAMILY_NAME, COLUMN_2);
          byte[] value = row.getValue(COLUMN_FAMILY_NAME, COLUMN_3);
          Pair temp = new Pair(Integer.parseInt(Bytes.toString(value)),Integer.parseInt(Bytes.toString(item)));
          print(Integer.toString(temp.first)+" "+ Integer.toString(temp.second));
        //   Pair temp = new Pair(1,2);
          pQueue.add(temp);
        }

        for(int i=0;i<K;i++)
        {
            // int res = pQueue.poll().second;
            result[i] = pQueue.poll().second; 
            print(Integer.toString(result[i]));
            
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
      return result;
  }

private static int interested(String itemID)  {
    int result = 0;
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
        System.out.println(result);
      return result;
  }

private static int view_count(String itemID)  {
    int result = 0;
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
        System.out.println(result);
      return result;
  }

  private static int popular()  {
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
            Integer value = Integer.parseInt(Bytes.toString(row.getValue(COLUMN_FAMILY_NAME, COLUMN_3)));

            if(map.get(item)!=null)
            {
                map.put(item,value+map.get(item));
            }
            else
            {
                map.put(item,value);
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
     System.out.println(result.first);
      return result.first;
  }


  /** Connects to Cloud Bigtable, runs some basic operations and prints the results. */
  private static void doHelloWorld(String projectId, String instanceId) {

    // [START bigtable_hw_connect]
    // Create the Bigtable connection, use try-with-resources to make sure it gets closed
    try (Connection connection = BigtableConfiguration.connect(projectId, instanceId)) {

      // The admin API lets us create, manage and delete tables
      Admin admin = connection.getAdmin();
      // [END bigtable_hw_connect]

      try {
        // [START bigtable_hw_create_table]
        // Create a table with a single column family
        HTableDescriptor descriptor = new HTableDescriptor(TableName.valueOf(TABLE_NAME));
        descriptor.addFamily(new HColumnDescriptor(COLUMN_FAMILY_NAME));

        print("Create table " + descriptor.getNameAsString());
        admin.createTable(descriptor);
        // [END bigtable_hw_create_table]

        // [START bigtable_hw_write_rows]
        // Retrieve the table we just created so we can do some reads and writes
        Table table = connection.getTable(TableName.valueOf(TABLE_NAME));

        // Write some rows to the table
        print("Write some greetings to the table");
        for (int i = 0; i < userID.size(); i++) {
          // Each row has a unique row key.
          //
          // Note: This example uses sequential numeric IDs for simplicity, but
          // this can result in poor performance in a production application.
          // Since rows are stored in sorted order by key, sequential keys can
          // result in poor distribution of operations across nodes.
          //
          // For more information about how to design a Bigtable schema for the
          // best performance, see the documentation:
          //
          //     https://cloud.google.com/bigtable/docs/schema-design
          String rowKey = userID.get(i) + "-" + itemID.get(i) ;

          // Put a single row into the table. We could also pass a list of Puts to write a batch.
          Put put = new Put(Bytes.toBytes(rowKey));
        //   put.addColumn(COLUMN_FAMILY_NAME, COLUMN_NAME, Bytes.toBytes(GREETINGS[i]));

          put.addColumn(COLUMN_FAMILY_NAME, COLUMN_1, Bytes.toBytes(userID.get(i)));
          put.addColumn(COLUMN_FAMILY_NAME, COLUMN_2, Bytes.toBytes(itemID.get(i)));
          put.addColumn(COLUMN_FAMILY_NAME, COLUMN_3, Bytes.toBytes(valueCounts.get(i)));

          table.put(put);
        }
        // [END bigtable_hw_write_rows]

        // [START bigtable_hw_get_by_key]
        // Get the first greeting by row key
        // String rowKey = "greeting0";
        // Result getResult = table.get(new Get(Bytes.toBytes(rowKey)));
        // String greeting = Bytes.toString(getResult.getValue(COLUMN_FAMILY_NAME, COLUMN_1));
        // System.out.println("Get a single greeting by row key");
        // System.out.printf("\t%s = %s\n", rowKey, greeting);

        print("Function Call");

        // top("2",2);
        // [END bigtable_hw_get_by_key]

        // [START bigtable_hw_scan_all]
        // Now scan across all rows.
        Scan scan = new Scan();

        print("Scan for all greetings:");
        ResultScanner scanner = table.getScanner(scan);
        for (Result row : scanner) {
          byte[] valueBytes = row.getValue(COLUMN_FAMILY_NAME, COLUMN_2);
          System.out.println('\t' + Bytes.toString(valueBytes));
        }
        // [END bigtable_hw_scan_all]

        // [START bigtable_hw_delete_table]
        // Clean up by disabling and then deleting the table
        // print("Delete the table");
        // admin.disableTable(table.getName());
        // admin.deleteTable(table.getName());
        // [END bigtable_hw_delete_table]
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

  public static void main(String[] args) {
    // Consult system properties to get project/instance

    try
    {
        String splitBy = ",";
        BufferedReader br = new BufferedReader(new FileReader("sample.csv"));
        try{
        String line = br.readLine();
          while((line = br.readLine()) != null){
               String[] b = line.split(splitBy);
               userID.add(b[0]);
               itemID.add(b[1]);
               valueCounts.add(b[2]);
               System.out.println(b[0]+b[1]+b[2]);
          }
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
    projectId = requiredProperty("bigtable.projectID");
    instanceId = requiredProperty("bigtable.instanceID");

    // doHelloWorld(projectId, instanceId);
    top("2",2);
    interested("2");
    view_count("2");
    popular();
    System.exit(0);

  }

  private static String requiredProperty(String prop) {
    String value = System.getProperty(prop);
    if (value == null) {
      throw new IllegalArgumentException("Missing required system property: " + prop);
    }
    return value;
  }
}
