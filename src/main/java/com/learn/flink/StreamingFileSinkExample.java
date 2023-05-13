package com.learn.flink;

import akka.stream.impl.io.FileSink;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.io.TextInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.DateTimeBucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import org.apache.flink.streaming.api.functions.source.FileProcessingMode;

import java.util.Locale;
import java.util.concurrent.TimeUnit;

public class StreamingFileSinkExample {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        String path="D:\\movielens-dataset\\ml-25m\\people-2ml.csv";
        env.enableCheckpointing(1000);
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);
        env.registerJobListener(new FlinkJobListener());
        env.enableCheckpointing(1000);
        env.setStateBackend((StateBackend) new FsStateBackend("file:///D:/checkpoint"));

        TextInputFormat textInputFormat = new TextInputFormat( new Path(path));

        long mapperStartTime = System.currentTimeMillis();
        DataStream<String> ds = env.readFile(textInputFormat, path, FileProcessingMode.PROCESS_ONCE, 1000, BasicTypeInfo.STRING_TYPE_INFO);
        DataStream<String> mappedString = ds.map(s -> {
            s=s.toUpperCase(Locale.ROOT);
            return s;
        });

        String outputPath = "D:\\movielens-dataset\\ml-25m\\output";
        final org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink<String> sink = org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink
                .forRowFormat(new Path(outputPath), new SimpleStringEncoder<String>("UTF-8"))
                .withOutputFileConfig(OutputFileConfig.builder().withPartPrefix("test").withPartSuffix("suffix")
                .build())
                .withRollingPolicy(DefaultRollingPolicy.builder()
                        .withInactivityInterval(60)
                        .withMaxPartSize(1024*1024*1024)
                        .withRolloverInterval(300)
                        .build())
                .build();
        long fileWriteStartTime = System.currentTimeMillis();

        ((SingleOutputStreamOperator<String>) mappedString).addSink(sink).setParallelism(1).name("FileTest");
        env.execute();
    }
}
