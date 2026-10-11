
package com.kpaatmik.springbatch.controller;

import java.util.UUID;

import org.springframework.batch.core.job.Job;
import org.springframework.batch.core.job.JobExecution;
import org.springframework.batch.core.job.parameters.JobParameters;
import org.springframework.batch.core.job.parameters.JobParametersBuilder;
import org.springframework.batch.core.launch.JobOperator;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/batch/employees")
public class EmployeeJobController {

    private final JobOperator jobOperator;
    private final Job employeeImportJob;

    public EmployeeJobController(
            JobOperator jobOperator,
            @Qualifier("employeeImportJob") Job employeeImportJob) {
        this.jobOperator = jobOperator;
        this.employeeImportJob = employeeImportJob;
    }

    @PostMapping("/import")
    public ResponseEntity<JobExecutionResponse> importEmployees()
            throws Exception {

        JobParameters jobParameters = new JobParametersBuilder()
                .addString("importId", UUID.randomUUID().toString())
                .toJobParameters();

        JobExecution execution =
                jobOperator.start(employeeImportJob, jobParameters);

        JobExecutionResponse response = new JobExecutionResponse(
                execution.getId(),
                execution.getStatus().name(),
                execution.getJobInstanceId()
        );

        return ResponseEntity.accepted().body(response);
    }

    public record JobExecutionResponse(
            Long executionId,
            String status,
            Long jobInstanceId
    ) {
    }
}
