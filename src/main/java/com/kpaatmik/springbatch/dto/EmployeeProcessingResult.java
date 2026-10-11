
package com.kpaatmik.springbatch.dto;

import com.kpaatmik.springbatch.model.Employee;
import com.kpaatmik.springbatch.model.EmployeeImportError;

public record EmployeeProcessingResult(Status status, Employee employee, EmployeeImportError error) {

	public enum Status {
		VALID, INVALID
	}

	public static EmployeeProcessingResult valid(Employee employee) {
		return new EmployeeProcessingResult(Status.VALID, employee, null);
	}

	public static EmployeeProcessingResult invalid(EmployeeImportError error) {

		return new EmployeeProcessingResult(Status.INVALID, null, error);
	}
}
