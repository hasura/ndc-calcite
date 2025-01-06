package com.example;

import org.apache.commons.cli.*;
import java.io.File;
import java.nio.file.Files;

public class DatabricksConnector {
    public static void main(String[] args) {
        Options options = setupCommandLineOptions();
        CommandLineParser parser = new DefaultParser();

        try {
            CommandLine cmd = parser.parse(options, args);
            if (cmd.hasOption("help")) {
                HelpFormatter formatter = new HelpFormatter();
                formatter.printHelp("DatabricksConnector", options);
                return;
            }

            String modelFile = cmd.getOptionValue("model");
            String accessToken = System.getenv("DATABRICKS_ACCESS_TOKEN");
            if (accessToken == null || accessToken.trim().isEmpty()) {
                throw new RuntimeException("DATABRICKS_ACCESS_TOKEN environment variable is not set");
            }

            // Register the Databricks JDBC driver
            Class.forName("com.databricks.client.jdbc.Driver");

            // Process the model file to replace the token
            String processedModel = processModelFile(modelFile, accessToken);
            File tempModelFile = createTempModelFile(processedModel);

            // Prepare SQLLine arguments
            String[] sqllineArgs = new String[] {
                "-u", "jdbc:calcite:model=" + tempModelFile.getAbsolutePath(),
                "-n", "token",
                "-p", accessToken,
                "-d", "com.databricks.client.jdbc.Driver",
                "--verbose=true",
                "--showHeader=true"
            };

            // Debug output
            System.out.println("Executing SQLLine with arguments:");
            for (String arg : sqllineArgs) {
                // Mask the password in the output
                if (arg.startsWith("--password=")) {
                    System.out.println("\t" + "--password=********");
                } else {
                    System.out.println("\t" + arg);
                }
            }

            // Launch SQLLine
            sqlline.SqlLine.main(sqllineArgs);

            // Cleanup
            tempModelFile.delete();
        } catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }
    }

    private static String processModelFile(String modelFile, String accessToken) throws Exception {
        String content = new String(Files.readAllBytes(new File(modelFile).toPath()));
        return content.replace("${DATABRICKS_ACCESS_TOKEN}", accessToken);
    }

    private static File createTempModelFile(String content) throws Exception {
        File tempFile = File.createTempFile("calcite-model", ".json");
        tempFile.deleteOnExit();
        Files.write(tempFile.toPath(), content.getBytes());
        return tempFile;
    }

    private static Options setupCommandLineOptions() {
        Options options = new Options();
        options.addOption(Option.builder("m")
                .longOpt("model")
                .hasArg()
                .required()
                .desc("Calcite model.json file path")
                .build());
        options.addOption("h", "help", false, "Show help");
        return options;
    }
}
