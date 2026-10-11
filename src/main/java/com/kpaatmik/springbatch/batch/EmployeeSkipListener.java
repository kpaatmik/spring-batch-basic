package com.kpaatmik.springbatch.batch;

import org.springframework.batch.core.listener.SkipListener;
import org.springframework.stereotype.Component;

import com.kpaatmik.springbatch.dto.EmployeeCsvRecord;
import com.kpaatmik.springbatch.model.Employee;
import com.kpaatmik.springbatch.model.EmployeeImportError;
import com.kpaatmik.springbatch.repository.EmployeeImportErrorRepository;

import lombok.AllArgsConstructor;

@Component
@AllArgsConstructor
public class EmployeeSkipListener implements SkipListener<EmployeeCsvRecord, Employee> {
	private final EmployeeImportErrorRepository employeeImportErrorRepository;

	@Override
	public void onSkipInProcess(EmployeeCsvRecord item, Throwable ex) {
		EmployeeImportError error = EmployeeImportError.builder().rawEmployeeId(item.employeeId())
				.rawEmail(item.email()).rawSalary(item.salary()).errorReason(ex.getMessage()).build();

		employeeImportErrorRepository.save(error);
	}

}
