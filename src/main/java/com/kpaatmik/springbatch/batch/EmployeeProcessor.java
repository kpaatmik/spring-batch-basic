package com.kpaatmik.springbatch.batch;

import java.math.BigDecimal;

import org.jspecify.annotations.Nullable;
import org.springframework.batch.infrastructure.item.ItemProcessor;
import org.springframework.stereotype.Component;

import com.kpaatmik.springbatch.dto.EmployeeCsvRecord;
import com.kpaatmik.springbatch.exception.InvalidEmployeeException;
import com.kpaatmik.springbatch.model.Employee;

@Component
public class EmployeeProcessor implements ItemProcessor<EmployeeCsvRecord, Employee> {

	@Override
	public @Nullable Employee process(EmployeeCsvRecord record) throws Exception {
		if (record.name() == null || record.name().isBlank()) {
			throw new InvalidEmployeeException("Name is mandatory");
		}
		if (record.email() == null || record.email().isBlank()) {
			throw new InvalidEmployeeException("Email is mandatory");
		}
		BigDecimal salary;
		try {
			salary = new BigDecimal(record.salary());
		} catch (NumberFormatException ex) {
			throw new InvalidEmployeeException("Salary must be numeric");
		}
		if (salary.compareTo(BigDecimal.ZERO) <= 0) {
			throw new InvalidEmployeeException("Salary must be greater than zero");
		}
		return new Employee(Long.parseLong(record.employeeId()), record.name().trim(), record.email().trim(), salary);
	}

}
