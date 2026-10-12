
package com.kpaatmik.springbatch.batch;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Locale;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.Resource;
import org.springframework.stereotype.Component;

import com.kpaatmik.springbatch.exception.InvalidInputFileException;

@Component
public class CsvFileValidator {

	private static final List<String> EXPECTED_HEADERS = List.of("employeeId", "name", "email", "salary");

	@Value("${app.batch.max-file-size-bytes:10485760}")
	private long maxFileSizeBytes;

	public void validate(Resource resource) {

		if (resource == null || !resource.exists()) {
			throw new InvalidInputFileException("Input CSV file does not exist");
		}
		if (!resource.isReadable()) {
			throw new InvalidInputFileException("Input CSV file is not readable");
		}
		String filename = resource.getFilename();
		if (filename == null || !filename.toLowerCase(Locale.ROOT).endsWith(".csv")) {
			throw new InvalidInputFileException("Input file must have a .csv extension");
		}
		try {
			long fileSize = resource.contentLength();
			if (fileSize == 0) {
				throw new InvalidInputFileException("Input CSV file is empty");
			}
			if (fileSize > maxFileSizeBytes) {
				throw new InvalidInputFileException("Input CSV file exceeds the maximum allowed size");
			}
			try (Reader reader = new BufferedReader(
					new InputStreamReader(resource.getInputStream(), StandardCharsets.UTF_8));
					CSVParser parser = CSVFormat.DEFAULT.builder().setHeader().setSkipHeaderRecord(true)
							.setIgnoreEmptyLines(false).get().parse(reader)) {

				// Validate only the CSV header structure.
				List<String> actualHeaders = parser.getHeaderNames();
				if (!EXPECTED_HEADERS.equals(actualHeaders)) {
					throw new InvalidInputFileException(
							"Invalid CSV header. Expected " + EXPECTED_HEADERS + " but received " + actualHeaders);
				}
			}

		} catch (InvalidInputFileException ex) {
			throw ex;
		} catch (IOException | IllegalArgumentException ex) {
			throw new InvalidInputFileException("Unable to parse the input CSV file: " + ex.getMessage(), ex);
		}
	}
}
