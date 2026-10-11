package com.kpaatmik.springbatch.repository;

import org.springframework.data.jpa.repository.JpaRepository;

import com.kpaatmik.springbatch.model.EmployeeImportError;

public interface EmployeeImportErrorRepository extends JpaRepository<EmployeeImportError, Long> {

}
