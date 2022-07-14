package com.asura.bigdata.akka;

import akka.stream.Attributes;
import akka.stream.Inlet;
import akka.stream.SinkShape;
import akka.stream.stage.AbstractInHandler;
import akka.stream.stage.GraphStage;
import akka.stream.stage.GraphStageLogic;
import com.asura.bigdata.conf.EndpointConf;
import com.asura.bigdata.model.SqlOperatorRow;
import com.zaxxer.hikari.HikariDataSource;
import lombok.extern.slf4j.Slf4j;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.List;

@Slf4j
public class JdbcSink extends GraphStage<SinkShape<SqlOperatorRow>> {
    private final EndpointConf jdbcConf;
    private HikariDataSource dataSource;
    public final Inlet<SqlOperatorRow> in = Inlet.create("JdbcSink.in");

    private final SinkShape<SqlOperatorRow> shape = SinkShape.of(in);

    public JdbcSink(EndpointConf jdbcConf) {
        this.jdbcConf = jdbcConf;
    }

    @Override
    public SinkShape<SqlOperatorRow> shape() {
        return shape;
    }

    @Override
    public GraphStageLogic createLogic(Attributes inheritedAttributes) {
        return new GraphStageLogic(shape()) {

            @Override
            public void preStart() {
                dataSource = jdbcConf.build();
                pull(in);
            }
            {
                setHandler(in, new AbstractInHandler() {
                    @Override
                    public void onPush() throws Exception {
                        SqlOperatorRow row = grab(in);
                        if (!row.isEmpty()) {
                            try (Connection c = dataSource.getConnection()) {
                                try (PreparedStatement ps = c.prepareStatement(row.getSql())) {
                                    List<Object> values = row.getValues();
                                    for (int i = 0; i < values.size(); i++) {
                                        ps.setObject(i + 1, values.get(i));
                                    }
                                    int affectedRows = ps.executeUpdate();
                                }

                            } catch (Exception e) {
                                log.error("", e);
                                cancel(in, e);
                                closeResource();
                                log.error("JdbcSink is closed!");
                            }
                        }
                        pull(in);
                    }
                });
            }
        };
    }

    private void closeResource() {
        if (null != dataSource) {
            if (!dataSource.isClosed()) {
                dataSource.close();
            }
        }
        dataSource = null;
    }

}
