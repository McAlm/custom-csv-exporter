package com.example.zeebe;

import io.camunda.zeebe.exporter.api.Exporter;
import io.camunda.zeebe.exporter.api.context.Controller;
import io.camunda.zeebe.protocol.record.Record;
import io.camunda.zeebe.protocol.record.intent.Intent;
import io.camunda.zeebe.protocol.record.intent.ProcessInstanceIntent;
import io.camunda.zeebe.protocol.record.value.ProcessInstanceModificationRecordValue;

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

    @Override
    public void open(Controller controller) {
        this.controller = controller;
        try {
            fileWriter = new FileWriter(CSV_FILE_PATH, true);
            // Write header if not already written
            if (!headerWritten) {
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
            String intent = null;

            // Handle different event types based on the intent
            Intent intentValue = record.getIntent();
            if (intentValue == ProcessInstanceIntent.ELEMENT_ACTIVATED ||
                intentValue == ProcessInstanceIntent.ELEMENT_COMPLETED ||
                intentValue == ProcessInstanceIntent.SEQUENCE_FLOW_TAKEN) {

                // Get the process instance key and element ID from the record value
                if (record.getValue() != null) {
                    try {
                        processInstanceKey = String.valueOf(record.getKey());
                        elementId = record.toJson();
                        System.out.println(elementId);
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
                        elementId,
                        intent
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