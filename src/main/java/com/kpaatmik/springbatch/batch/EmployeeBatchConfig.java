package com.kpaatmik.springbatch.batch;

import java.util.concurrent.Future;

import org.springframework.batch.core.job.Job;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.Step;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.infrastructure.item.file.FlatFileItemReader;
import org.springframework.batch.infrastructure.repeat.RepeatStatus;
import org.springframework.batch.integration.async.AsyncItemProcessor;
import org.springframework.batch.integration.async.AsyncItemWriter;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.ApplicationRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.transaction.PlatformTransactionManager;

import com.kpaatmik.springbatch.dto.EmployeeCsvRecord;
import com.kpaatmik.springbatch.dto.EmployeeProcessingResult;

@Configuration
public class EmployeeBatchConfig {
	@Bean
	public Step employeeImportStep(JobRepository jobRepository, PlatformTransactionManager transactionManager,
			FlatFileItemReader<EmployeeCsvRecord> employeeReader,
			AsyncItemProcessor<EmployeeCsvRecord, EmployeeProcessingResult> asyncEmployeeProcessor,
			AsyncItemWriter<EmployeeProcessingResult> asyncEmployeeWriter) {
		return new StepBuilder("employeeImportStep", jobRepository)
				.<EmployeeCsvRecord, Future<EmployeeProcessingResult>>chunk(5).transactionManager(transactionManager)
				.reader(employeeReader).processor(asyncEmployeeProcessor).writer(asyncEmployeeWriter).build();
	}

	@Bean
	public Step fileValidationStep(JobRepository jobRepository, PlatformTransactionManager platformTransactionManager,
			CsvFileValidator csvFileValidator) {
		return new StepBuilder("fileValidationStep", jobRepository).tasklet((contribution, chunkContext) -> {
			csvFileValidator.validate(new ClassPathResource("employees.csv"));
			return RepeatStatus.FINISHED;
		}, platformTransactionManager).allowStartIfComplete(true).build();
	}

	@Bean
	public Job employeeImportJob(JobRepository jobRepository, @Qualifier("fileValidationStep") Step fileValidationStep,
			@Qualifier("employeeImportStep") Step employeeImportStep) {
		return new JobBuilder("employeeImportJob", jobRepository).start(fileValidationStep).next(employeeImportStep)
				.build();
	}

	@Bean
	public ApplicationRunner inspectJobRepository(JobRepository jobRepository) {
		return args -> System.out.println("JobRepository implementation: " + jobRepository.getClass().getName());
	}

}
