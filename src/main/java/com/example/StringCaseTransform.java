package com.example;

import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.plugin.PluginConfig;
import io.cdap.cdap.etl.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.Collections;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Transform that can transforms specific fields to lowercase or uppercase.
 */
@Plugin(type = Transform.PLUGIN_TYPE)
@Name(StringCaseTransform.NAME)
@Description("Transforms configured fields to lowercase or uppercase.")
public class StringCaseTransform extends Transform<StructuredRecord, StructuredRecord> {
    private static Logger log = LoggerFactory.getLogger(StringCaseTransform.class);
    public static final String NAME = "StringCase";
    private final Conf config;
    private Set<String> upperFields;
    private Set<String> lowerFields;

    /**
     * Config properties for the plugin.
     */
    public static class Conf extends PluginConfig {
        private static final String SPLIT_PATTERN = "\\s*,\\s*";

        // nullable means this property is optional
        @Nullable
        @Description("A comma separated list of fields to uppercase. Each field must be of type String.")
        private String upperFields;

        @Nullable
        @Description("A comma separated list of fields to lowercase. Each field must be of type String.")
        private String lowerFields;

        private Set<String> getUpperFields() {
            return parseToSet(upperFields);
        }

        private Set<String> getLowerFields() {
            return parseToSet(lowerFields);
        }

        private Set<String> parseToSet(String str) {
            if (str == null || str.isEmpty()) {
                return Collections.EMPTY_SET;
            } else {
                return Arrays.stream(str.split(SPLIT_PATTERN)).collect(Collectors.toSet());
            }
        }
    }

    public StringCaseTransform(Conf config) {
        this.config = config;
    }

    // configurePipeline is called only once, when the pipeline is deployed. Static validation should be done here.
    @Override
    public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
        StageConfigurer stageConfigurer = pipelineConfigurer.getStageConfigurer();
        // the output schema is always the same as the input schema
        Schema inputSchema = stageConfigurer.getInputSchema();
        // if schema is null, that means it is either not known until runtime, or it is variable
        if (inputSchema != null) {
            // if the input schema is constant and known at configure time, check that all configured fields are strings
            config.getUpperFields().forEach(fieldName -> validateFieldIsString(inputSchema, fieldName));
            config.getLowerFields().forEach(fieldName -> validateFieldIsString(inputSchema, fieldName));
        }
        stageConfigurer.setOutputSchema(inputSchema);
    }

    // initialize is called once at the start of each pipeline run
    @Override
    public void initialize(TransformContext context) throws Exception {
        log.info("Init started");
        upperFields = config.getUpperFields();
        lowerFields = config.getLowerFields();
    }

    // transform is called once for each record that goes into this stage
    @Override
    public void transform(StructuredRecord record, Emitter<StructuredRecord> emitter) throws Exception {
        log.info("Transform started");
        StructuredRecord.Builder builder = StructuredRecord.builder(record.getSchema());
        for (Schema.Field field : record.getSchema().getFields()) {
            String fieldName = field.getName();
            if (upperFields.contains(fieldName)) {
                builder.set(fieldName, record.get(fieldName).toString().toUpperCase());
            } else if (lowerFields.contains(fieldName)) {
                builder.set(fieldName, record.get(fieldName).toString().toLowerCase());
            } else {
                builder.set(fieldName, record.get(fieldName));
            }
        }
        emitter.emit(builder.build());
    }

    private void validateFieldIsString(Schema schema, String fieldName) {
        Schema.Field inputField = schema.getField(fieldName);
        if (inputField == null) {
            throw new IllegalArgumentException(String.format("Field '%s' does not exist in input schema %s.", fieldName, schema));
        }
        Schema fieldSchema = inputField.getSchema();
        Schema.Type fieldType = fieldSchema.isNullable() ? fieldSchema.getNonNullable().getType() : fieldSchema.getType();
        if (fieldType != Schema.Type.STRING) {
            throw new IllegalArgumentException(
                    String.format("Field '%s' is of illegal type %s. Must be of type %s.",
                            fieldName, fieldType, Schema.Type.STRING));
        }
    }
}