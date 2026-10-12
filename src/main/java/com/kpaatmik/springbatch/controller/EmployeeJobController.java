
package com.kpaatmik.springbatch.controller;

import java.util.Map;
import java.util.UUID;

import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.job.Job;
import org.springframework.batch.core.job.JobExecution;
import org.springframework.batch.core.job.parameters.JobParameters;
import org.springframework.batch.core.job.parameters.JobParametersBuilder;
import org.springframework.batch.core.launch.JobOperator;
import org.springframework.batch.core.launch.JobRestartException;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import lombok.AllArgsConstructor;

@RestController
@AllArgsConstructor
@RequestMapping("/api/batch/employees")
public class EmployeeJobController {

	private final JobOperator jobOperator;
	private final Job employeeImportJob;
	private final JobRepository jobRepository;

	@PostMapping("/import")
	public ResponseEntity<JobExecutionResponse> importEmployees() throws Exception {

		JobParameters jobParameters = new JobParametersBuilder().addString("importId", UUID.randomUUID().toString())
				.toJobParameters();

		JobExecution execution = jobOperator.start(employeeImportJob, jobParameters);

		JobExecutionResponse response = new JobExecutionResponse(execution.getId(), execution.getStatus().name(),
				execution.getJobInstanceId());

		return ResponseEntity.accepted().body(response);
	}

	@PostMapping("/restart/{executionId}")
	public ResponseEntity<Map<String, Object>> restart(@PathVariable long executionId) {

		JobExecution failedExecution = jobRepository.getJobExecution(executionId);

		if (failedExecution == null) {
			return ResponseEntity.notFound().build();
		}

		if (failedExecution.getStatus() != BatchStatus.FAILED && failedExecution.getStatus() != BatchStatus.STOPPED) {

			return ResponseEntity.badRequest()
					.body(Map.of("message", "Only FAILED or STOPPED executions can be restarted", "status",
							failedExecution.getStatus().toString()));
		}

		try {
			JobExecution restartedExecution = jobOperator.restart(failedExecution);

			return ResponseEntity.accepted().body(Map.of("previousExecutionId", executionId, "newExecutionId",
					restartedExecution.getId(), "status", restartedExecution.getStatus().toString()));

		} catch (JobRestartException ex) {
			return ResponseEntity.status(409)
					.body(Map.of("message",
							"Cannot restart this job instance. It may already " + "be running or completed.",
							"executionId", executionId));
		}
	}

	public record JobExecutionResponse(Long executionId, String status, Long jobInstanceId) {
	}
}
