package com.kpaatmik.springbatch.repository;

import java.sql.PreparedStatement;
import java.util.ArrayList;
import java.util.List;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import com.kpaatmik.springbatch.model.Employee;

import lombok.RequiredArgsConstructor;

@Repository
@RequiredArgsConstructor
public class EmployeeWriterRepositoryImpl implements EmployeeWriterRepository {

	private final JdbcTemplate jdbcTemplate;

	@Override
	public void insertIgnoreDuplicates(List<? extends Employee> employees) {

		if (employees.isEmpty()) {
			return;
		}

		// Convert List<? extends Employee> to List<Employee>
		List<Employee> employeeList = new ArrayList<>(employees);

		String sql = """
				INSERT INTO employees (employee_id, name, email, salary)
				VALUES (?, ?, ?, ?)
				ON CONFLICT DO NOTHING
				""";

		jdbcTemplate.batchUpdate(sql, employeeList, employeeList.size(), (PreparedStatement ps, Employee employee) -> {
			ps.setLong(1, employee.getEmployeeId());
			ps.setString(2, employee.getName());
			ps.setString(3, employee.getEmail());
			ps.setBigDecimal(4, employee.getSalary());
		});
	}
}