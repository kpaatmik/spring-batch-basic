package com.kpaatmik.springbatch.exception;

public class InvalidInputFileException extends RuntimeException {

	public InvalidInputFileException(String message) {
		super(message);
	}

	public InvalidInputFileException(String message, Throwable cause) {
		super(message, cause);
	}
}