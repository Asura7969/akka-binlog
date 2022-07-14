package com.asura.bigdata.akka;

import akka.stream.Attributes;
import akka.stream.Outlet;
import akka.stream.SourceShape;
import akka.stream.stage.AbstractOutHandler;
import akka.stream.stage.GraphStage;
import akka.stream.stage.GraphStageLogic;
import com.asura.bigdata.debezium.BinlogFactory;
import com.asura.bigdata.debezium.SourceConf;
import com.asura.bigdata.model.Event;
import io.debezium.engine.ChangeEvent;
import io.debezium.engine.DebeziumEngine;
import org.apache.kafka.common.utils.ThreadUtils;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class BinlogSource extends GraphStage<SourceShape<Event>> {
    private final SourceConf conf;
    public final Outlet<Event> out = Outlet.create("BinlogEvent.out");
    private final SourceShape<Event> shape = SourceShape.of(out);

    private ExecutorService executor = Executors.newFixedThreadPool(
            1,
            ThreadUtils.createThreadFactory(
                    this.getClass().getSimpleName() + "-%d", false));

    public BinlogSource(SourceConf conf) {
        this.conf = conf;
    }

    @Override
    public SourceShape<Event> shape() {
        return shape;
    }

    @Override
    public GraphStageLogic createLogic(Attributes inheritedAttributes) {
        return new GraphStageLogic(shape()) {
            private final BinlogFactory factory = create();
            {
                setHandler(
                        out,
                        new AbstractOutHandler() {
                            @Override
                            public void onPull() {
                                try {
                                    push(out, factory.fetchRow());
                                } catch (InterruptedException e) {
                                    stop();
                                    fail(out, e);
                                }
                            }
                        });
            }

            @Override
            public void preStart() throws Exception {
//                AsyncCallback<Done> callback =
//                        createAsyncCallback(
//                                new Procedure<Done>() {
//                                    @Override
//                                    public void apply(Done param) throws Exception {
//                                        completeStage();
//                                    }
//                                });
                System.out.println("执行：preStart");
            }
        };
    }

    public void stop() {
        if (executor != null) {
            executor.shutdown();
            // Best effort wait for any get() and set() tasks (and caller's callbacks) to complete.
            try {
                executor.awaitTermination(30, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            if (!executor.shutdownNow().isEmpty()) {
                throw new ConnectException(
                        "Failed to stop AkkaOffsetBackingStore. Exiting without cleanly "
                                + "shutting down pending tasks and/or callbacks.");
            }
            executor = null;
        }
    }

    private BinlogFactory create() {
        BinlogFactory s = new BinlogFactory();
        DebeziumEngine<ChangeEvent<SourceRecord, SourceRecord>> engine = s.build(conf);
        executor.execute(engine);
        return s;
    }
}
