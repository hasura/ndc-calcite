package com.hasura;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import org.yaml.snakeyaml.Yaml;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public class ConfigPreprocessor {


    private static final List<String> ATTRIBUTES_TO_CHANGE = Arrays.asList("directory", "pathToRootCert");
    private static void processAttributes(Object data, String inputFilePath) {
        if (data instanceof Map) {
            //noinspection unchecked
            for (Map.Entry<String, Object> entry : ((Map<String, Object>) data).entrySet()) {
                if (ATTRIBUTES_TO_CHANGE.contains(entry.getKey())) {
                    String directory = (String) entry.getValue();
                    String fullFilePath = Paths.get(inputFilePath).toAbsolutePath().getParent().resolve(directory).normalize().toString();
                    entry.setValue(fullFilePath);
                } else {
                    processAttributes(entry.getValue(), inputFilePath);
                }
            }
        } else if (data instanceof Iterable) {
            for (Object obj : (Iterable) data) {
                processAttributes(obj, inputFilePath);
            }
        }
    }

    public static String preprocessConfig(String inputFilePath) throws IOException {
        // Read the template file
        String content = new String(Files.readAllBytes(Paths.get(inputFilePath)));

        // Replace placeholders with environment variable values
        Map<String, String> env = System.getenv();
        for (Map.Entry<String, String> entry : env.entrySet()) {
            String placeholder = "${" + entry.getKey() + "}";
            content = content.replace(placeholder, entry.getValue());
        }

        // Determine if the file is JSON or YAML
        boolean isJson = inputFilePath.endsWith(".json");

        // Parse the content
        ObjectMapper objectMapper = new ObjectMapper();
        Object data;
        if (isJson) {
            data = objectMapper.readValue(content, Object.class);
        } else {
            Yaml yaml = new Yaml();
            data = yaml.load(content);
        }

        // Process directories
        processAttributes(data, inputFilePath);

        // Convert the data to JSON formatted string
        objectMapper.enable(SerializationFeature.INDENT_OUTPUT);
        String jsonString = objectMapper.writeValueAsString(data);

        // Generate a unique filename with UUID
        String uniqueFilename = "resolved_config_" + UUID.randomUUID() + ".json";

        // Write the JSON string to a file in a temporary directory
        Path tempDir = Files.createTempDirectory("config");
        Path tempFile = Files.createFile(Paths.get(tempDir.toString(), uniqueFilename));
        Files.write(tempFile, jsonString.getBytes());

        // Return the full path of the resolved file
        return tempFile.toAbsolutePath().toString();
    }
}
