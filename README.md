# Custom Zeebe CSV Exporter

This project demonstrates how to create a custom exporter for Camunda 8 (Zeebe) that generates a CSV event log with the specified columns.

## Project Structure

- `src/main/java/com/example/zeebe/CsvExporter.java`: The main exporter class
- `src/main/java/com/example/zeebe/CsvExporterExample.java`: Example demonstrating how to use the exporter
- `pom.xml`: Maven build file with dependencies

## Dependencies

The project uses the following dependencies:
- Zeebe Exporter API (version 8.8.0-alpha5)
- Apache Commons CSV (for CSV handling)

## Usage

### Running the Example

To run the example, use the following command:

```sh
mvn compile exec:java -Dexec.mainClass="com.example.zeebe.CsvExporterExample"
```

This will generate a `zeebe_events.csv` file with sample event data.

### Implementing Your Own Exporter

To implement your own exporter, extend the `CsvExporter` class and override the `export` method to handle different types of Zeebe records. The example demonstrates how to write events to a CSV file with the required columns:

- sequence: The sequence number of the event
- timestamp: The time the event occurred
- value.processInstanceKey: The process instance key from the event
- value.elementId: The element ID from the event
- intent: The action (e.g., ELEMENT_COMPLETED, SEQUENCE_FLOW_TAKEN)

### Registering with Zeebe

In a real scenario, you would register your exporter with the Zeebe engine. This typically involves:

1. Implementing the `Exporter` interface from the Zeebe Exporter API
2. Registering the exporter with the Zeebe engine configuration
3. Handling different types of records in the `export` method

For more details, see the [Camunda 8 documentation](https://docs.camunda.io/docs/apis-exporters/).

## License

This project is licensed under the MIT License.