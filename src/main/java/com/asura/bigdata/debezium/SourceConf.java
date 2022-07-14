package com.asura.bigdata.debezium;

import java.util.List;

public class SourceConf {

    private String host;
    private Integer port;
    private String user;
    private String password;
    private String serverId;
    private String offsetStorageFileName;
    private String databaseHistoryFileName;
    private List<String> databases;
    private List<String> tables;

    public String getDatabases() {
        return String.join(",", databases);
    }

    public SourceConf setDatabases(List<String> databases) {
        this.databases = databases;
        return this;
    }

    public String getTables() {
        return String.join(",", tables);
    }

    public SourceConf setTables(List<String> tables) {
        this.tables = tables;
        return this;
    }

    public String getHost() {
        return host;
    }

    public SourceConf setHost(String host) {
        this.host = host;
        return this;
    }

    public Integer getPort() {
        return port;
    }

    public SourceConf setPort(Integer port) {
        this.port = port;
        return this;
    }

    public String getUser() {
        return user;
    }

    public SourceConf setUser(String user) {
        this.user = user;
        return this;
    }

    public String getPassword() {
        return password;
    }

    public SourceConf setPassword(String password) {
        this.password = password;
        return this;
    }

    public String getServerId() {
        return serverId;
    }

    public SourceConf setServerId(String serverId) {
        this.serverId = serverId;
        return this;
    }

    public String getOffsetStorageFileName() {
        return offsetStorageFileName;
    }

    public SourceConf setOffsetStorageFileName(String offsetStorageFileName) {
        this.offsetStorageFileName = offsetStorageFileName;
        return this;
    }

    public String getDatabaseHistoryFileName() {
        return databaseHistoryFileName;
    }

    public SourceConf setDatabaseHistoryFileName(String databaseHistoryFileName) {
        this.databaseHistoryFileName = databaseHistoryFileName;
        return this;
    }
}
