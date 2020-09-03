package com.mapr.tools.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Table;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;

public class Demo2 {

  private static final Logger log = LogManager.getLogger(Demo2.class.getName());

  public void execute() throws IOException {
    Configuration configuration = HBaseConfiguration.create();

    Connection connection = ConnectionFactory.createConnection(configuration);
    Table table = connection.getTable(TableName.valueOf("emp"));

//    connection.getAdmin().getTableRegions(table.getName()).stream().forEach(
//            x -> {
//              System.out.println(x.getRegionNameAsString());
//
//            });


//    Collection<ServerName> rs = connection.getAdmin().getClusterStatus().getServers();
//    for (ServerName r : rs) {
//      System.out.println(r.getHostname());
//      System.out.println(r.getServerName());
//      System.out.println(r.getStartcode());
//      System.out.println("+++++++++++++++++");
//    }

    System.out.println("Tables:");
    HTableDescriptor[] tableDescriptor = connection.getAdmin().listTables();

    // printing all the table names.
    for (int i=0; i<tableDescriptor.length;i++ ){
      System.out.println(tableDescriptor[i].getNameAsString());
    }


    connection.getRegionLocator(table.getName()).getAllRegionLocations().stream().forEach(x -> {
      System.out.println(x.getHostname());
      System.out.println(x.getServerName());
      System.out.println(x.getRegionInfo().getRegionNameAsString());
      System.out.println("+++++++++++++++++");

    });

//
//    Scan scan = new Scan();
//    scan.addColumn(Bytes.toBytes("personal data"), Bytes.toBytes("name"));
//    ResultScanner scanner = table.getScanner(scan);
//
//    for (Result result = scanner.next(); result != null; result = scanner.next()) {
//      byte[] valueBytes = result.getValue(Bytes.toBytes("personal data"), Bytes.toBytes("name"));
//      System.out.println("Found row : " + Bytes.toString(valueBytes));
//    }
//
//    scanner.close();

    table.close();

  }

  public static void main(String[] args) throws IOException {
    new Demo2().execute();
  }

}
