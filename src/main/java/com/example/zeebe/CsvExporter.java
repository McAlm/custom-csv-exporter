package com.example.zeebe;

import io.camunda.zeebe.exporter.api.Exporter;
import io.camunda.zeebe.exporter.api.context.Controller;
import io.camunda.zeebe.protocol.record.Record;
import io.camunda.zeebe.protocol.record.intent.Intent;
import io.camunda.zeebe.protocol.record.intent.ProcessInstanceIntent;

import java.io.FileWriter;
import java.io.IOException;
import java.time.Instant;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Custom CSV exporter for Zeebe events.
 */
public class CsvExporter implements Exporter {

    private String CSV_FILE_PATH;
    private AtomicInteger sequence;
    private FileWriter fileWriter;
    private boolean headerWritten;

    Controller controller;

    public CsvExporter() {
        CSV_FILE_PATH = "zeebe_events.csv";
        sequence = new AtomicInteger(0);
        headerWritten = false;
    }

    private boolean fileHasHeaders() {
        java.io.File file = new java.io.File(CSV_FILE_PATH);
        if (!file.exists()) {
            return false;
        }

        try (java.io.BufferedReader reader = new java.io.BufferedReader(new java.io.FileReader(file))) {
            String firstLine = reader.readLine();
            if (firstLine != null) {
                // Check if the first line contains our header columns
                return firstLine.contains("sequence") &&
                       firstLine.contains("timestamp") &&
                       firstLine.contains("value.processInstanceKey") &&
                       firstLine.contains("value.elementId") &&
                       firstLine.contains("intent");
            }
        } catch (IOException e) {
            System.err.println("Error checking CSV headers: " + e.getMessage());
        }
        return false;
    }

    @Override
    public void open(Controller controller) {
        this.controller = controller;
        try {
            fileWriter = new FileWriter(CSV_FILE_PATH, true);
            // Write header only if file doesn't exist or doesn't have headers
            java.io.File file = new java.io.File(CSV_FILE_PATH);
            if (!file.exists() || !fileHasHeaders()) {
                writeCsvRecord(
                    "sequence",
                    "timestamp",
                    "value.processInstanceKey",
                    "value.elementId",
                    "intent"
                );
                headerWritten = true;
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void export(final Record<?> record) {
        try {
            String processInstanceKey = null;
            String elementId = null;
            String intentStr = null;

            // Handle different event types based on the intent
            Intent intentValue = record.getIntent();
            if (intentValue == ProcessInstanceIntent.ELEMENT_ACTIVATED ||
                intentValue == ProcessInstanceIntent.ELEMENT_COMPLETED ||
                intentValue == ProcessInstanceIntent.SEQUENCE_FLOW_TAKEN) {

                // Get the process instance key and element ID from the record value
                if (record.getValue() != null) {
                    try {
                        processInstanceKey = String.valueOf(record.getKey());

                        // Extract elementId and intent from JSON string
                        String jsonStr = record.toJson();
                        System.out.println(jsonStr);

                        // Simple JSON parsing without external dependencies
                        int valueStart = jsonStr.indexOf("\"value\":") + 8;
                        int valueEnd = jsonStr.indexOf('}', valueStart);
                        if (valueStart > 0 && valueEnd > 0) {
                            String valueJson = jsonStr.substring(valueStart, valueEnd);

                            // Extract elementId
                            int elementIdStart = valueJson.indexOf("\"elementId\":\"") + 13;
                            int elementIdEnd = valueJson.indexOf('\"', elementIdStart);
                            if (elementIdStart > 0 && elementIdEnd > 0) {
                                elementId = valueJson.substring(elementIdStart, elementIdEnd);
                            }

                            // Extract intent
                            int intentStart = jsonStr.indexOf("\"intent\":\"") + 10;
                            int intentEnd = jsonStr.indexOf('\"', intentStart);
                            if (intentStart > 0 && intentEnd > 0) {
                                intentStr = jsonStr.substring(intentStart, intentEnd);
                            }
                        }
                    } catch (Exception e) {
                        System.err.println("Error processing record value: " + e.getMessage());
                    }
                }
            }

            if (processInstanceKey != null) {
                writeCsvRecord(
                        sequence.incrementAndGet(),
                        Instant.ofEpochMilli(record.getTimestamp()).toString(),
                        processInstanceKey,
                        elementId != null ? elementId : "",
                        intentStr != null ? intentStr : ""
                );
            }
        } catch (IOException e) {
            System.err.println("Error writing to CSV: " + e.getMessage());
        }
    }

    @Override
    public void close() {
        try {
            if (fileWriter != null) {
                fileWriter.flush();
                fileWriter.close();
            }
        } catch (IOException e) {
            System.err.println("Error closing file writer: " + e.getMessage());
        }
    }

    private void writeCsvRecord(Object... values) throws IOException {
        writeLine(String.join(",", String.valueOf(values[0]), (String)values[1], (String)values[2], (String)values[3], (String)values[4]));
    }

    private void writeLine(String line) throws IOException {
        fileWriter.write(line);
        fileWriter.write(System.lineSeparator());
    }
}