package com.asura.bigdata.conf;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;

@Accessors(chain=true)
@Data
@NoArgsConstructor
@AllArgsConstructor
public class EndpointConf {

    private String jdbcUrl;
    private String userName;
    private String password;
    private String database;
    private String table;

    public HikariDataSource build() {
        HikariConfig hikariConfig = new HikariConfig();
        hikariConfig.setPoolName("akka-binlog-mysql-sink");
        hikariConfig.setDriverClassName("com.mysql.cj.jdbc.Driver");
        hikariConfig.setJdbcUrl(jdbcUrl);
        hikariConfig.setUsername(userName);
        hikariConfig.setPassword(password);
        hikariConfig.setMinimumIdle(2);
        hikariConfig.setMaximumPoolSize(5);

        return new HikariDataSource(hikariConfig);

    }
}
