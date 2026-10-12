
package com.kpaatmik.springbatch.batch;

import org.springframework.batch.integration.async.AsyncItemProcessor;
import org.springframework.batch.integration.async.AsyncItemWriter;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.task.AsyncTaskExecutor;

import com.kpaatmik.springbatch.dto.EmployeeCsvRecord;
import com.kpaatmik.springbatch.dto.EmployeeProcessingResult;

@Configuration
public class AsyncProcessorConfig {

	@Bean
	public AsyncItemProcessor<EmployeeCsvRecord, EmployeeProcessingResult> asyncEmployeeProcessor(
			EmployeeProcessor employeeProcessor, AsyncTaskExecutor employeeProcessorTaskExecutor) {
		AsyncItemProcessor<EmployeeCsvRecord, EmployeeProcessingResult> asyncProcessor = new AsyncItemProcessor<>(
				employeeProcessor);
		asyncProcessor.setTaskExecutor(employeeProcessorTaskExecutor);
		return asyncProcessor;
	}

	@Bean
	public AsyncItemWriter<EmployeeProcessingResult> asyncEmployeeWriter(EmployeeResultWriter employeeResultWriter) {
		return new AsyncItemWriter<>(employeeResultWriter);
	}
}
