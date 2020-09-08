package com.mapr.tools.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class Info {

  private static final Logger log = LogManager.getLogger(Info.class.getName());
  private List<RegionInfoDto> regionInfoDtoList = new ArrayList<>();

  public void execute() throws IOException {
    Configuration configuration = HBaseConfiguration.create();

    Connection connection = ConnectionFactory.createConnection(configuration);
    Admin admin = connection.getAdmin();

    HTableDescriptor[] tableDescriptor = admin.listTables();

    regionInfoDtoList.clear();
    for (int i=0; i<tableDescriptor.length;i++ ){
      Table table = connection.getTable(tableDescriptor[i].getTableName());
      regionInfoDtoList.addAll(getRegionInfos(table,connection));
      table.close();
    }
  }

  private List<RegionInfoDto> getRegionInfos(Table table, Connection connection) throws IOException {
    return connection.getRegionLocator(table.getName()).getAllRegionLocations().stream().map(x -> {

      RegionInfoDto regionInfoDto = new RegionInfoDto();
      regionInfoDto.setTable(table.getName().getNameAsString());
      regionInfoDto.setRegion(x.getRegionInfo().getRegionNameAsString());
      regionInfoDto.setServer(x.getServerName().getHostname());

      return regionInfoDto;
    }).collect(Collectors.toList());

  }

  public static void main(String[] args) throws IOException {
    Info info = new Info();
    info.execute();

    Map<String, List<RegionInfoDto>> grouped = info.regionInfoDtoList.stream()
            .collect(Collectors.groupingBy(RegionInfoDto::getTable, Collectors.toList()));

    System.out.println("TOTALS:");
    System.out.println("-------");
    System.out.println("REGIONS: " + grouped.values().stream().flatMap(x-> x.stream()).count());
    System.out.println("------------------------");
    System.out.println("SERVER  /   REGION COUNT");
    Map<String, Long> forEnv = info.regionInfoDtoList.stream().collect(Collectors.groupingBy(RegionInfoDto::getServer, Collectors.counting()));
    forEnv.forEach((j,k)-> {
      System.out.println(j + "  /   " + k);
    });

    System.out.println();

    System.out.println("TABLES:");
    System.out.println("-------");
    grouped.forEach((x,y) -> {
      System.out.println("NAME: " + x);
      System.out.println("REGIONS: " + y.size());
      System.out.println("------------------------");
      System.out.println("SERVER  /   REGION COUNT");
      Map<String, Long> forServer = y.stream().collect(Collectors.groupingBy(RegionInfoDto::getServer, Collectors.counting()));
      forServer.forEach((j,k)-> {
        System.out.println(j + "  /   " + k);
      });
    });

  }

}
