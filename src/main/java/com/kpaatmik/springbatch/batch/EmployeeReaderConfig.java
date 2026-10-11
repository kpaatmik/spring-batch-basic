package com.kpaatmik.springbatch.batch;

import org.springframework.batch.infrastructure.item.file.FlatFileItemReader;
import org.springframework.batch.infrastructure.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;

import com.kpaatmik.springbatch.dto.EmployeeCsvRecord;

@Configuration
public class EmployeeReaderConfig {

	@Bean
	public FlatFileItemReader<EmployeeCsvRecord> employeeReader() {
		return new FlatFileItemReaderBuilder<EmployeeCsvRecord>().name("employeeReader")
				.resource(new ClassPathResource("employees.csv")).linesToSkip(1).delimited().delimiter(",")
				.names("employeeId", "name", "email", "salary").strict(false)
				.fieldSetMapper(fieldSet -> new EmployeeCsvRecord(fieldSet.readString("employeeId"),
						fieldSet.readString("name"), fieldSet.readString("email"), fieldSet.readString("salary")))
				.build();

	}
}
