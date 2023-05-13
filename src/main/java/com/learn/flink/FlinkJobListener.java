package com.learn.flink;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.core.execution.JobListener;

import javax.annotation.Nullable;

@Slf4j
public class FlinkJobListener implements JobListener {
    @Override
    public void onJobSubmitted(@Nullable JobClient jobClient, @Nullable Throwable throwable) {
        if (throwable != null) {
            log.error("Job failed to submit", throwable);
            return;
        }
        log.info("Job submitted successfully with jobid: {}", jobClient.getJobID());
    }

    @Override
    public void onJobExecuted(@Nullable JobExecutionResult jobExecutionResult, @Nullable Throwable throwable) {
        if (throwable != null) {
            log.error("Job failed to finish ", throwable);
            return;
        }
        log.info("Job completed successfully");
        log.info("Job Details:" + jobExecutionResult.getJobExecutionResult());
    }
}
