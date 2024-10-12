package com.hasura;

import java.sql.*;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.regex.Pattern;
import java.util.regex.Matcher;


/**
 * The StatementPreparer class is responsible for preparing a SQL statement by replacing
 * marked up strings and indexed question marks with the actual values.
 */
class StatementPreparer {

    static String STRING_MARKER = "__UTF8__";
    static String PARAM_MARKER = "?";

    /**
     * Prepares a SQL statement by replacing marked up strings and indexed question marks with the actual values.
     *
     * @param input      The SQL statement to prepare.
     * @param connection The database connection.
     * @return A PreparedStatement object representing the prepared SQL statement.
     * @throws SQLException If an error occurs while preparing the statement.
     */
    public static PreparedStatement prepare(String input, Connection connection) throws SQLException {
        ArrayList<Object> extractedStrings = extractMarkedUpStrings(input);
        String modifiedInput = replaceWithIndexedQuestionMarks(input, extractedStrings);
        ArrayList<Object> extractedStringParams = new ArrayList<>();
        modifiedInput = findParams(modifiedInput, extractedStrings, extractedStringParams);
        PreparedStatement preparedStatement = connection.prepareStatement(modifiedInput);

        for (int i = 0; i < extractedStringParams.size(); i++) {
            Object item = extractedStringParams.get(i);
            if (item instanceof String) {
                preparedStatement.setString(i + 1, (String) item);
            } else if (item instanceof Timestamp) {
                preparedStatement.setTimestamp(i + 1, (Timestamp) item);
            }
        }
        return preparedStatement;
    }

    private static String findParams(String input, ArrayList<Object> extractedStrings, ArrayList<Object> params) {
        Pattern pattern = Pattern.compile("\\?(\\d+)\\?");
        Matcher matcher = pattern.matcher(input);
        StringBuffer sb = new StringBuffer();
        while (matcher.find()) {
            String value = matcher.group().replaceAll("\\?", "");
            int index = Integer.parseInt(value);
            params.add(extractedStrings.get(index));
            matcher.appendReplacement(sb, PARAM_MARKER);
        }
        matcher.appendTail(sb);
        return sb.toString();
    }

    private static ArrayList<Object> extractMarkedUpStrings(String input) {
        String marker = STRING_MARKER;
        ArrayList<Object> extractedList = new ArrayList<>();

        int startIndex = input.indexOf(marker);
        while (startIndex != -1) {
            int endIndex = input.indexOf(marker, startIndex + marker.length());
            if (endIndex != -1) {
                String extracted = input.substring(startIndex + marker.length(), endIndex);
                extractedList.add(extracted);
            }
            startIndex = input.indexOf(marker, endIndex + marker.length());
        }
        return extractedList;
    }

    /**
     * Replaces marked up strings with indexed question marks in the input SQL statement.
     * But makes a special exception for UTC formatted dates, in which case it
     * converts them to ANSI SQL dates in local time.
     *
     * @param input The SQL statement to process.
     * @param extractedStrings The list of extracted marked-up strings.
     * @return The input SQL statement with marked up strings replaced by indexed question marks.
     */
    private static String replaceWithIndexedQuestionMarks(String input, ArrayList<Object> extractedStrings) {
        Pattern pattern = Pattern.compile("^\\d{4}-\\d{1,2}-\\d{1,2}([T\\s]\\d{2}:\\d{2}:\\d{2}(\\.\\d{3}))Z$");
        DateTimeFormatter rfcFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSX");
        DateTimeFormatter localFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");
        for (int i = 0; i < extractedStrings.size(); ++i) {
            Matcher matcher = pattern.matcher((String) extractedStrings.get(i));
            if (matcher.matches()) {
                // if it seems like it is a date - we are going to tyr and make it a date constant
                try {
                    // if the pattern follows the exact pattern that comes from the hasura NDC
                    // we will convert it to a ANSI SQL timestamp.
                    // Otherwise, we will convert it to a string constant verbatim.
                    ZonedDateTime zonedDateTime = ZonedDateTime.parse((String) extractedStrings.get(i), rfcFormatter);
                    input = input.replace(
                            STRING_MARKER + extractedStrings.get(i) + STRING_MARKER,
                            PARAM_MARKER + i + PARAM_MARKER
                    );
                    extractedStrings.set(i, Timestamp.from(zonedDateTime.toInstant()));
                } catch(Exception ignored) {
                    input = input.replace(
                            STRING_MARKER + extractedStrings.get(i) + STRING_MARKER,
                            PARAM_MARKER + i + PARAM_MARKER
                    );
                }
            // if it's not a date constant, we will convert it into string parameter
            } else {
                input = input.replace(
                        STRING_MARKER + extractedStrings.get(i) + STRING_MARKER,
                        PARAM_MARKER + i + PARAM_MARKER
                );
            }
        }
        return input;
    }
}
