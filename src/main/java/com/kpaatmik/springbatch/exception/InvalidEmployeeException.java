package com.kpaatmik.springbatch.exception;

public class InvalidEmployeeException extends RuntimeException {
	public InvalidEmployeeException(String message) {
		super(message);
	}
}
