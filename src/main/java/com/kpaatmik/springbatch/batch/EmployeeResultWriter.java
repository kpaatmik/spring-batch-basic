package com.kpaatmik.springbatch.batch;

import java.util.ArrayList;
import java.util.List;

import org.springframework.batch.infrastructure.item.Chunk;
import org.springframework.batch.infrastructure.item.ItemWriter;
import org.springframework.stereotype.Component;

import com.kpaatmik.springbatch.dto.EmployeeProcessingResult;
import com.kpaatmik.springbatch.model.Employee;
import com.kpaatmik.springbatch.model.EmployeeImportError;
import com.kpaatmik.springbatch.repository.EmployeeImportErrorRepository;
import com.kpaatmik.springbatch.repository.EmployeeWriterRepository;

@Component
public class EmployeeResultWriter implements ItemWriter<EmployeeProcessingResult> {

	private final EmployeeWriterRepository employeeWriterRepository;
	private final EmployeeImportErrorRepository errorRepository;

	public EmployeeResultWriter(EmployeeWriterRepository employeeWriterRepository,
			EmployeeImportErrorRepository errorRepository) {

		this.employeeWriterRepository = employeeWriterRepository;
		this.errorRepository = errorRepository;
	}

	@Override
	public void write(Chunk<? extends EmployeeProcessingResult> chunk) {

		List<Employee> validEmployees = new ArrayList<>();
		List<EmployeeImportError> invalidEmployees = new ArrayList<>();

		for (EmployeeProcessingResult result : chunk.getItems()) {

			if (result.status() == EmployeeProcessingResult.Status.VALID) {
				validEmployees.add(result.employee());
			} else {
				invalidEmployees.add(result.error());
			}
		}

		// Insert valid employees, ignoring database conflicts
		if (!validEmployees.isEmpty()) {
			employeeWriterRepository.insertIgnoreDuplicates(validEmployees);
		}

		// Persist validation errors
		if (!invalidEmployees.isEmpty()) {
			errorRepository.saveAll(invalidEmployees);
		}
	}
}
