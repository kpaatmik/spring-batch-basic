package com.kpaatmik.springbatch.repository;

import java.util.List;

import com.kpaatmik.springbatch.model.Employee;

public interface EmployeeWriterRepository {

	void insertIgnoreDuplicates(List<? extends Employee> employees);
}