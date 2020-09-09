package com.mapr.tools.hbase;

import org.apache.commons.cli.*;
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
    private Configuration configuration;
    private Connection connection;

    public Info() throws IOException {
        this.configuration = HBaseConfiguration.create();
        this.connection = ConnectionFactory.createConnection(configuration);
    }

    public Connection getConnection() {
        return connection;
    }

    public void gatherForAll() throws IOException {
        Admin admin = connection.getAdmin();

        HTableDescriptor[] tableDescriptor = admin.listTables();

        regionInfoDtoList.clear();
        for (int i = 0; i < tableDescriptor.length; i++) {
            Table table = connection.getTable(tableDescriptor[i].getTableName());
            regionInfoDtoList.addAll(getRegionInfos(table, connection));
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

    private static void printAllTables(Map<String, List<RegionInfoDto>> grouped) {
        System.out.println("-------");
        System.out.println("TABLES INFORMATION:");
        System.out.println("-------");
        grouped.forEach(Info::printTable);
    }


    private static void printTable(String name, List<RegionInfoDto> forTable) {
        System.out.println("TABLE NAME: " + name);
        System.out.println("REGIONS: " + forTable.size());
        System.out.println("------------------------");
        System.out.println("SERVER  /   REGION COUNT");
        Map<String, Long> forServer = forTable.stream().collect(Collectors.groupingBy(RegionInfoDto::getServer, Collectors.counting()));
        forServer.forEach((j, k) -> {
            System.out.println(j + "  /   " + k);
        });
    }

    private static void printTotals(List<RegionInfoDto> all, Map<String, List<RegionInfoDto>> grouped) {
        System.out.println("---------------------------------");
        System.out.println("TOTAL REGIONS IN THE ENVIRONMENT: " + grouped.values().stream().flatMap(x -> x.stream()).count());
        System.out.println("---------------------------------");
        System.out.println("SERVER  /   REGION COUNT");
        Map<String, Long> forEnv = all.stream().collect(Collectors.groupingBy(RegionInfoDto::getServer, Collectors.counting()));
        forEnv.forEach((j, k) -> {
            System.out.println(j + "  /   " + k);
        });
        System.out.println();
    }

    public static void main(String[] args) throws IOException, ParseException {

        Options options = new Options();
        options
                .addOption("t", true, "Prints info for this table.")
                .addOption("a", "all", false, "prints all info gathered for the current environment.")
                .addOption("e", "environment", false, "prints only totals for the current environment.");


        CommandLineParser parser = new DefaultParser();
        CommandLine line = parser.parse(options, args);

        if (line.getOptions().length == 0) {
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp("HBase Environment", options);
            return;
        }

        Info info = new Info();
        info.gatherForAll();

        Map<String, List<RegionInfoDto>> grouped = info.regionInfoDtoList.stream()
                .collect(Collectors.groupingBy(RegionInfoDto::getTable, Collectors.toList()));

        if (line.hasOption("t")) {
            String tableName = line.getOptionValue("t");
            printTable(tableName, grouped.get(tableName));
            return;
        }

        if (line.hasOption("a")) {
            printTotals(info.regionInfoDtoList, grouped);
            printAllTables(grouped);
            return;
        }

        if (line.hasOption("e")) {
            printTotals(info.regionInfoDtoList, grouped);
            return;
        }

    }


}
