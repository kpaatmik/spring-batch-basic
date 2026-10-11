package com.kpaatmik.springbatch.dto;

public record EmployeeCsvRecord(
        String employeeId,
        String name,
        String email,
        String salary
) {
}