
package com.kpaatmik.springbatch.batch;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

import org.springframework.batch.infrastructure.item.ItemProcessor;
import org.springframework.stereotype.Component;

import com.kpaatmik.springbatch.dto.EmployeeCsvRecord;
import com.kpaatmik.springbatch.dto.EmployeeProcessingResult;
import com.kpaatmik.springbatch.model.Employee;
import com.kpaatmik.springbatch.model.EmployeeImportError;

@Component
public class EmployeeProcessor implements ItemProcessor<EmployeeCsvRecord, EmployeeProcessingResult> {

	@Override
	public EmployeeProcessingResult process(EmployeeCsvRecord record) {

		List<String> errors = new ArrayList<>();

		String rawId = record.employeeId();
		String name = record.name();
		String email = record.email();
		String rawSalary = record.salary();

		Long employeeId = null;
		BigDecimal salary = null;

		// Employee ID validation
		if (rawId == null || rawId.isBlank()) {
			errors.add("Employee ID is mandatory");
		} else {
			try {
				employeeId = Long.parseLong(rawId.trim());
			} catch (NumberFormatException ex) {
				errors.add("Employee ID must be a valid integer");
			}
		}

		// Name validation
		if (name == null || name.isBlank()) {
			errors.add("Name is mandatory");
		}

		// Email validation
		if (email == null || email.isBlank()) {
			errors.add("Email is mandatory");
		} else if (!email.trim().matches("^[A-Za-z0-9+_.-]+@[A-Za-z0-9.-]+$")) {
			errors.add("Email format is invalid");
		}

		// Salary validation
		if (rawSalary == null || rawSalary.isBlank()) {
			errors.add("Salary is mandatory");
		} else {
			try {
				salary = new BigDecimal(rawSalary.trim());

				if (salary.compareTo(BigDecimal.ZERO) <= 0) {
					errors.add("Salary must be greater than zero");
				}
			} catch (NumberFormatException ex) {
				errors.add("Salary must be numeric");
			}
		}

//		if (rawId.equals("246"))
//			throw new RuntimeException("Error happened");

		// Return an error result instead of throwing for invalid data
		if (!errors.isEmpty()) {
			EmployeeImportError error = new EmployeeImportError();

			error.setRawEmployeeId(rawId);
			error.setRawName(name);
			error.setRawEmail(email);
			error.setRawSalary(rawSalary);
			error.setErrorReason(String.join("; ", errors));

			return EmployeeProcessingResult.invalid(error);
		}

		String normalizedEmail = email.trim().toLowerCase(Locale.ROOT);

		Employee employee = new Employee(employeeId, name.trim(), normalizedEmail, salary);

		return EmployeeProcessingResult.valid(employee);
	}
}
