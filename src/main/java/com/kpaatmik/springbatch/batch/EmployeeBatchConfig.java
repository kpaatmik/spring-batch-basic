package com.kpaatmik.springbatch.batch;

import org.springframework.batch.core.job.Job;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.Step;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.infrastructure.item.file.FlatFileItemReader;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.ApplicationRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.transaction.PlatformTransactionManager;

import com.kpaatmik.springbatch.dto.EmployeeCsvRecord;
import com.kpaatmik.springbatch.exception.InvalidEmployeeException;
import com.kpaatmik.springbatch.model.Employee;

@Configuration
public class EmployeeBatchConfig {
	@Bean
	public Step employeeImportStep(JobRepository jobRepository, PlatformTransactionManager platformTransactionManager,
			FlatFileItemReader<EmployeeCsvRecord> empReader, EmployeeProcessor employeeProcessor,
			EmployeeWriter employeeWriter, EmployeeSkipListener employeeSkipListener) {
		return new StepBuilder("employeeImportStep", jobRepository).<EmployeeCsvRecord, Employee>chunk(5)
				.transactionManager(platformTransactionManager).reader(empReader).processor(employeeProcessor)
				.writer(employeeWriter).faultTolerant().skip(InvalidEmployeeException.class)
				.skipListener(employeeSkipListener).skipLimit(50).build();
	}

	@Bean
	public Job employeeImportJob(JobRepository jobRepository,
			@Qualifier("employeeImportStep") Step employeeImportStep) {
		return new JobBuilder("employeeImportJob", jobRepository).start(employeeImportStep).build();
	}

	@Bean
	public ApplicationRunner inspectJobRepository(JobRepository jobRepository) {
		return args -> System.out.println("JobRepository implementation: " + jobRepository.getClass().getName());
	}

}
