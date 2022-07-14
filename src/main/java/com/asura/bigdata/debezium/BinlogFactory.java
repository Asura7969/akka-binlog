package com.asura.bigdata.debezium;

import com.asura.bigdata.model.Event;
import io.debezium.connector.SnapshotRecord;
import io.debezium.data.Envelope;
import io.debezium.embedded.Connect;
import io.debezium.engine.ChangeEvent;
import io.debezium.engine.DebeziumEngine;
import io.debezium.heartbeat.Heartbeat;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.ArrayBlockingQueue;

@Slf4j
public class BinlogFactory {

    private final ArrayBlockingQueue<Event> queue = new ArrayBlockingQueue<>(10000);
    private final JsonDebeziumDeserializationSchema deserialization = new JsonDebeziumDeserializationSchema();

    public DebeziumEngine<ChangeEvent<SourceRecord, SourceRecord>> build(SourceConf cConf) {
        final Properties props = new Properties();
        props.setProperty("name", "engine");
        props.setProperty("connector.class", "io.debezium.connector.mysql.MySqlConnector");
        props.setProperty("database.include.list", cConf.getDatabases());
        props.setProperty("table.include.list", cConf.getTables());
        props.setProperty("offset.storage", "org.apache.kafka.connect.storage.FileOffsetBackingStore");

        /*  Kafka config  */
//        props.setProperty("offset.storage", "org.apache.kafka.connect.storage.KafkaOffsetBackingStore");
//        props.setProperty("offset.storage.topic", "akka_binlog_topic");
//        props.setProperty("offset.storage.partitions", "3");
//        props.setProperty("offset.storage.replication.factor", "2");
        props.setProperty("offset.storage.file.filename", cConf.getOffsetStorageFileName());
        props.setProperty("offset.flush.interval.ms", "60000");
        /* begin connector properties */
        props.setProperty("database.hostname", cConf.getHost());
        props.setProperty("database.port", cConf.getPort().toString());
        props.setProperty("database.user", cConf.getUser());
        props.setProperty("database.password", cConf.getPassword());
        props.setProperty("database.server.id", cConf.getServerId());
        props.setProperty("database.server.name", "my-app-connector");
        props.setProperty("database.history",
                "io.debezium.relational.history.FileDatabaseHistory");
        props.setProperty("database.history.file.filename",
                cConf.getDatabaseHistoryFileName());

        // Create the engine with this configuration ...
        DebeziumEngine<ChangeEvent<SourceRecord, SourceRecord>> engine;

        try {
            engine = DebeziumEngine.create(Connect.class)
                    .using(props)
                    .notifying(new Transform())
                    .build();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return engine;
    }

    private class Transform implements DebeziumEngine.ChangeConsumer<ChangeEvent<SourceRecord, SourceRecord>> {

        @Override
        public void handleBatch(List<ChangeEvent<SourceRecord, SourceRecord>> events,
                                DebeziumEngine.RecordCommitter<ChangeEvent<SourceRecord, SourceRecord>> committer) throws InterruptedException {
            for (ChangeEvent<SourceRecord, SourceRecord> event : events) {
                SourceRecord record = event.value();
                if (isHeartbeatEvent(record) || null == record.value()) {
                    // todo:offset
                    continue;
                }
                try {
                    Event e = deserialization.deserialize(record);
                    if (queue.offer(e)) {
                        committer.markProcessed(event);
                    }
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }


    public Event fetchRow() throws InterruptedException {
        return queue.take();
    }

    private boolean isHeartbeatEvent(SourceRecord record) {
        String topic = record.topic();
        return topic != null && topic.startsWith(Heartbeat.HEARTBEAT_TOPICS_PREFIX.defaultValueAsString());
    }


    private boolean isSnapshotRecord(SourceRecord record) {
        Struct value = (Struct) record.value();
        if (value != null) {
            Struct source = value.getStruct(Envelope.FieldName.SOURCE);
            SnapshotRecord snapshotRecord = SnapshotRecord.fromSource(source);
            return SnapshotRecord.TRUE == snapshotRecord;
        }
        return false;
    }


}
