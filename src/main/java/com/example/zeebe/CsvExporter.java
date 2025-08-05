package com.example.zeebe;

import java.io.FileWriter;
import java.io.IOException;

import io.camunda.zeebe.exporter.api.Exporter;
import io.camunda.zeebe.exporter.api.context.Controller;
import io.camunda.zeebe.protocol.record.Record;
import io.camunda.zeebe.protocol.record.intent.Intent;
import io.camunda.zeebe.protocol.record.intent.ProcessInstanceIntent;
import io.camunda.zeebe.protocol.record.intent.VariableIntent;

/**
 * Custom CSV exporter for Zeebe events.
 */
public class CsvExporter implements Exporter {

    private String CSV_FILE_PATH;
    private FileWriter fileWriter;
    private static final String CSV_DELIMITER = "@@"; // Separator for CSV values

    // Controller to manage the exporter lifecycle
    private Controller controller;

    public CsvExporter() {
        CSV_FILE_PATH = "zeebe_events.csv";
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
                return firstLine.contains("position") &&
                        firstLine.contains("timestamp") &&
                        firstLine.contains("intent") &&
                        firstLine.contains("valueType") &&
                        firstLine.contains("processDefinitionKey") &&
                        firstLine.contains("processInstanceKey") &&
                        firstLine.contains("tenantId") &&
                        firstLine.contains("bpmnProcessId") &&
                        firstLine.contains("bpmnElementType") &&
                        firstLine.contains("elementId") &&
                        firstLine.contains("variableName") &&
                        firstLine.contains("variableValue");
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
                        "position",
                        "timestamp",
                        "intent",
                        "valueType",
                        "processDefinitionKey",
                        "processInstanceKey",
                        "tenantId",
                        "bpmnProcessId",
                        "bpmnElementType",
                        "elementId",
                        "variableName",
                        "variableValue");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void export(final Record<?> record) {

        ZeebeEvent zeebeEvent = null;

        // Handle different event types based on the intent
        Intent intentValue = record.getIntent();
        if (intentValue == ProcessInstanceIntent.ELEMENT_ACTIVATED ||
                intentValue == ProcessInstanceIntent.ELEMENT_COMPLETED ||
                intentValue == ProcessInstanceIntent.SEQUENCE_FLOW_TAKEN ||
                intentValue == VariableIntent.CREATED ||
                intentValue == VariableIntent.UPDATED) {

            // Get the process instance key and element ID from the record value
            if (record.getValue() != null) {
                try {
                    zeebeEvent = new ZeebeEvent(record);
                    writeCsvRecord(zeebeEvent.getPosition(),
                            zeebeEvent.getTimestamp(),
                            zeebeEvent.getIntent(),
                            zeebeEvent.getValueType(),
                            zeebeEvent.getProcessDefinitionKey(),
                            zeebeEvent.getProcessInstanceKey(),
                            zeebeEvent.getTenantId(),
                            zeebeEvent.getBpmnProcessId(),
                            zeebeEvent.getBpmnElementType(),
                            zeebeEvent.getElementId(),
                            zeebeEvent.getVariableName(),
                            zeebeEvent.getVariableValue());
                } catch (IOException e) {
                    System.err.println("Error writing to CSV: " + e.getMessage());

                } catch (Exception e) {
                    System.err.println("Error processing record value: " + e.getMessage());
                }
            }
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

    private void writeCsvRecord(String... values) throws IOException {
        //join the values with commas and write to the file
        String[] stringValues = new String[values.length];
        for (int i = 0; i < values.length; i++) {
            stringValues[i] = String.valueOf(values[i]);
        }
        writeLine(String.join(CSV_DELIMITER, stringValues));
    }

    private void writeLine(String line) throws IOException {
        fileWriter.write(line);
        fileWriter.write(System.lineSeparator());
        fileWriter.flush();
    }
}