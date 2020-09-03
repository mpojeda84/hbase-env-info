package com.mapr.tools.hbase;

public class RegionInfoDto {

    private String table;
    private String region;
    private String server;

    public String getTable() {
        return table;
    }

    public void setTable(String table) {
        this.table = table;
    }

    public String getRegion() {
        return region;
    }

    public void setRegion(String region) {
        this.region = region;
    }

    public String getServer() {
        return server;
    }

    public void setServer(String server) {
        this.server = server;
    }

    @Override
    public String toString() {
        return "RegionInfoDto{" +
                "table='" + table + '\'' +
                ", region='" + region + '\'' +
                ", server='" + server + '\'' +
                '}';
    }
}
