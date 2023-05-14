package com.learn.flink;

import com.learn.flink.entity.PersonDTO;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.java.io.TextInputFormat;
import org.apache.flink.api.java.utils.MultipleParameterTool;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.BasePathBucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;

import java.util.Locale;

@Slf4j
public class FileSinkFilterExample {

    public static void main(String[] args)  throws Exception {

        MultipleParameterTool params = MultipleParameterTool.fromArgs(args);
        StreamExecutionEnvironment exeEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        exeEnv.setRuntimeMode(RuntimeExecutionMode.BATCH); // use batch mode, !
        exeEnv.registerJobListener(new FlinkJobListener());

        String inputFile ="D:\\movielens-dataset\\ml-25m\\people-2ml.csv";
        String outputFileLocation = "D:\\movielens-dataset\\ml-25m\\output";
        if (params.has("input")) {
            inputFile = params.get("input");
            log.info("Setting input file  to:"+inputFile);
        }else {
            log.info("No input param passed. Input set to default file :" + inputFile);
        }
        if (params.has("output")) {
            outputFileLocation = params.get("output");
            log.info("Setting output file location to:"+outputFileLocation);
        }else {
            log.info("No output param passed. output set to default file location:" + outputFileLocation);
        }

        FileSink<String> fileSink = FileSink.forRowFormat(new Path(outputFileLocation),new SimpleStringEncoder<String>())
                .withBucketAssigner(new BasePathBucketAssigner<>())
                .withRollingPolicy(DefaultRollingPolicy.builder()
                        .withInactivityInterval(60)
                        .withMaxPartSize(1024*1024*1024)
                        .withRolloverInterval(300)
                        .build())
                .build();

        TextInputFormat textInputFormat = new TextInputFormat( new Path(inputFile));

        DataStream<String> ds =  exeEnv.createInput(textInputFormat);
        DataStream<PersonDTO> entityStream  = ds.map(new EnitityMapper());
        DataStream<PersonDTO> filteredEntityStream = entityStream.filter(new EntityFilter());
        DataStream<String> result = filteredEntityStream.map( e -> e.toString().toUpperCase(Locale.ROOT));
        result.sinkTo(fileSink).setParallelism(1);
        exeEnv.execute();

    }

}

class EnitityMapper implements MapFunction<String, PersonDTO> {

    @Override
    public PersonDTO map(String inputRecord) throws Exception {
        String[] recordAttributes = inputRecord.split(",");

        return new PersonDTO( recordAttributes[0], recordAttributes[1],
                recordAttributes[2], recordAttributes[3], recordAttributes[4],
                recordAttributes[5], recordAttributes[6],recordAttributes[7], recordAttributes[8]);
    }
}

class EntityFilter implements FilterFunction<PersonDTO> {

    @Override
    public boolean filter(PersonDTO personDTO) throws Exception {
        return personDTO.getEmail().equalsIgnoreCase("Email") ? false : true;

    }
}

