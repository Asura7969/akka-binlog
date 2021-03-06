package com.asura.bigdata;

import akka.actor.ActorSystem;
import akka.stream.javadsl.Source;
import com.asura.bigdata.conf.EndpointConf;
import com.asura.bigdata.helper.TableSchema;
import com.google.common.collect.Lists;
import com.asura.bigdata.akka.BinlogSource;
import com.asura.bigdata.akka.JdbcSink;
import com.asura.bigdata.debezium.SourceConf;
import com.asura.bigdata.model.SqlOperatorRow;

import static com.asura.bigdata.helper.EventHelper.*;
import static com.asura.bigdata.helper.JdbcHelper.SqlType.BIGINT;
import static com.asura.bigdata.helper.JdbcHelper.SqlType.VARCHAR;

import com.google.common.collect.Maps;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.Map;

@Slf4j
public class Runner {

    private static final String IP = "localhost";

    public static void main(String[] args) {

        SourceConf sourceConf = new SourceConf()
                .setHost(IP)
                .setPort(3306)
                .setUser("binlog")
                .setPassword("123456")
                .setDatabases(Lists.newArrayList("binlog"))
                .setTables(Lists.newArrayList("binlog.t_city"))
                .setServerId("85744")
                // todo: 添加存储类型 kafka or file, 自动判断并设置文件路径
                .setDatabaseHistoryFileName("E:/IdeaProjects/akka-binlog/src/main/resources/db/dbhistory.dat")
                .setOffsetStorageFileName("E:/IdeaProjects/akka-binlog/src/main/resources/db/dboffset.dat");

        EndpointConf sinkConf = new EndpointConf()
                .setJdbcUrl(String.format("jdbc:mysql://%s:3306/binlog?useUnicode=true&characterEncoding=UTF-8", IP))
                .setUserName("binlog")
                .setPassword("123456")
                .setDatabase("binlog")
                .setTable("t_city_clone");

        TableSchema sinkSchema = new TableSchema()
                .addField("id", BIGINT)
                .addField("name", VARCHAR);

        String sinkTable = String.format("%s.%s", sinkConf.getDatabase(), sinkConf.getTable());

        ActorSystem system = ActorSystem.create("akka-binlog-stream");

        BinlogSource binlogEventSource = new BinlogSource(sourceConf);
        Source.fromGraph(binlogEventSource)
                .map((event) -> {
                    List<Map.Entry<String, Object>> after = event.getListAfter();
                    List<Map.Entry<String, Object>> before = event.getListBefore();
                    switch (event.getOp()) {
                        case "c":
                            return handleInsert(sinkTable, after);
                        case "u":
                            return handleUpdate(sinkTable, event.getBefore(), event.getAfter());
                        case "d":
                            return handleDelete(sinkTable, before);
                        default:
                            log.warn("Not support operation: {}, event: {}", event.getOp(), event);
                    }
                    return SqlOperatorRow.empty();
                })
                .runWith(new JdbcSink(sinkConf, sinkSchema), system);
//                .runForeach(System.out::println, system);

//        doneCompletionStage.thenRun(system::terminate);
    }

}
