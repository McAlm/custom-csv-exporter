package com.example.zeebe;

import java.time.Instant;

import io.camunda.zeebe.protocol.record.Record;
import io.camunda.zeebe.protocol.record.value.ProcessInstanceRecordValue;
import io.camunda.zeebe.protocol.record.value.VariableRecordValue;

// POJO for Zeebe event attributes
public class ZeebeEvent {
    private String processDefinitionKey;
    private String bpmnProcessId;
    private String processInstanceKey;
    private String elementId;
    private String tenantId;
    private String bpmnElementType;
    private String variableName;
    private String variableValue;
    private String position;
    private String intent;
    private String valueType;
    private String timestamp;

    public ZeebeEvent(final Record<?> record) {

        if (record.getValue() instanceof ProcessInstanceRecordValue) {

            ProcessInstanceRecordValue value = (ProcessInstanceRecordValue) record.getValue();
            initFromProcessInstanceRecordValue(value);
        } else if (record.getValue() instanceof VariableRecordValue) {
            VariableRecordValue value = (VariableRecordValue) record.getValue();
            initFromVariableRecordValue(value);
        }

        this.position = String.valueOf(record.getPosition());
        this.intent = record.getIntent().name();
        this.valueType = record.getValueType().name();
        this.timestamp = Instant.ofEpochMilli(record.getTimestamp()).toString();
    }

    private void initFromVariableRecordValue(VariableRecordValue value) {
        this.processDefinitionKey = String.valueOf(value.getProcessDefinitionKey());
        this.bpmnProcessId = value.getBpmnProcessId();
        this.processInstanceKey = String.valueOf(value.getProcessInstanceKey());
        this.elementId = "";
        this.tenantId = value.getTenantId();
        this.bpmnElementType = "";
        this.variableName = value.getName();
        this.variableValue = value.getValue();
    }

    private void initFromProcessInstanceRecordValue(ProcessInstanceRecordValue value) {
        this.processDefinitionKey = String.valueOf(value.getProcessDefinitionKey());
        this.bpmnProcessId = value.getBpmnProcessId();
        this.processInstanceKey = String.valueOf(value.getProcessInstanceKey());
        this.elementId = value.getElementId();
        this.tenantId = value.getTenantId();
        this.bpmnElementType = value.getBpmnElementType().getElementTypeName().orElse("");
        this.variableName = "";
        this.variableValue = "";
    }

    public String getProcessDefinitionKey() {
        return processDefinitionKey;
    }

    public void setProcessDefinitionKey(String processDefinitionKey) {
        this.processDefinitionKey = processDefinitionKey;
    }

    public String getBpmnProcessId() {
        return bpmnProcessId;
    }

    public void setBpmnProcessId(String bpmnProcessId) {
        this.bpmnProcessId = bpmnProcessId;
    }

    public String getProcessInstanceKey() {
        return processInstanceKey;
    }

    public void setProcessInstanceKey(String processInstanceKey) {
        this.processInstanceKey = processInstanceKey;
    }

    public String getElementId() {
        return elementId;
    }

    public void setElementId(String elementId) {
        this.elementId = elementId;
    }

    public String getTenantId() {
        return tenantId;
    }

    public void setTenantId(String tenantId) {
        this.tenantId = tenantId;
    }

    public String getBpmnElementType() {
        return bpmnElementType;
    }

    public void setBpmnElementType(String bpmnElementType) {
        this.bpmnElementType = bpmnElementType;
    }

    public String getVariableName() {
        return variableName;
    }

    public void setVariableName(String variableName) {
        this.variableName = variableName;
    }

    public String getVariableValue() {
        return variableValue;
    }

    public void setVariableValue(String variableValue) {
        this.variableValue = variableValue;
    }

    public String getPosition() {
        return position;
    }

    public void setPosition(String position) {
        this.position = position;
    }

    public String getIntent() {
        return intent;
    }

    public void setIntent(String intent) {
        this.intent = intent;
    }

    public String getValueType() {
        return valueType;
    }

    public void setValueType(String valueType) {
        this.valueType = valueType;
    }

    public String getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(String timestamp) {
        this.timestamp = timestamp;
    }
}