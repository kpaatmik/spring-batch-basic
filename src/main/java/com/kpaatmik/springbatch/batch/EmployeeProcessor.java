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

		if (record.employeeId() == null || record.employeeId().isBlank()) {
			throw new InvalidEmployeeException("Employee ID is mandatory");
		}

		if (record.name() == null || record.name().isBlank()) {
			throw new InvalidEmployeeException("Name is mandatory");
		}

		if (record.email() == null || record.email().isBlank()) {
			throw new InvalidEmployeeException("Email is mandatory");
		}

		// Validate salary before creating BigDecimal.
		if (record.salary() == null || record.salary().isBlank()) {
			throw new InvalidEmployeeException("Salary is mandatory");
		}

		BigDecimal salary;

		try {
			salary = new BigDecimal(record.salary().trim());
		} catch (NumberFormatException ex) {
			throw new InvalidEmployeeException("Salary must be numeric");
		}

		if (salary.compareTo(BigDecimal.ZERO) <= 0) {
			throw new InvalidEmployeeException("Salary must be greater than zero");
		}

		Long employeeId;

		try {
			employeeId = Long.parseLong(record.employeeId().trim());
		} catch (NumberFormatException ex) {
			throw new InvalidEmployeeException("Employee ID must be a valid integer");
		}

		return new Employee(employeeId, record.name().trim(), record.email().trim(), salary);
	}
}
