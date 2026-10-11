package com.kpaatmik.springbatch.batch;

import org.springframework.batch.infrastructure.item.Chunk;
import org.springframework.batch.infrastructure.item.ItemWriter;
import org.springframework.stereotype.Component;

import com.kpaatmik.springbatch.model.Employee;
import com.kpaatmik.springbatch.repository.EmployeeRepository;

import lombok.RequiredArgsConstructor;

@Component
@RequiredArgsConstructor
public class EmployeeWriter implements ItemWriter<Employee> {
	private final EmployeeRepository employeeRepository;

	@Override
	public void write(Chunk<? extends Employee> chunk) throws Exception {
		employeeRepository.saveAll(chunk.getItems());
	}
}
