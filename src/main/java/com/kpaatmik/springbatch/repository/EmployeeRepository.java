package com.kpaatmik.springbatch.repository;

import org.springframework.data.jpa.repository.JpaRepository;

import com.kpaatmik.springbatch.model.Employee;

public interface EmployeeRepository extends JpaRepository<Employee, Long> {

}
