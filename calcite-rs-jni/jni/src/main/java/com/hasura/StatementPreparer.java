package com.hasura;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Date;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.util.ArrayList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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
            } else if (item instanceof Date) {
                preparedStatement.setDate(i + 1, (Date) item);
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
     * @param input            The SQL statement to process.
     * @param extractedStrings The list of extracted marked-up strings.
     * @return The input SQL statement with marked up strings replaced by indexed question marks.
     */
    private static String replaceWithIndexedQuestionMarks(String input, ArrayList<Object> extractedStrings) {
        Pattern pattern = Pattern.compile("^\\d{4}-\\d{1,2}-\\d{1,2}([T\\s]\\d{2}:\\d{2}:\\d{2}(\\.\\d{3}))Z$");
        DateTimeFormatter rfcFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSX");
        for (int i = 0; i < extractedStrings.size(); ++i) {
            Matcher matcher = pattern.matcher((String) extractedStrings.get(i));
            if (matcher.matches()) {
                // if it seems like it is a date - we are going to try and make it a date constant
                try {
                    // if the pattern follows the exact pattern that comes from the hasura NDC
                    // we will convert it to a ANSI SQL timestamp.
                    // Otherwise, we will convert it to a string constant verbatim.
                    ZonedDateTime zonedDateTime = ZonedDateTime.parse((String) extractedStrings.get(i), rfcFormatter);
                    zonedDateTime = zonedDateTime.withZoneSameInstant(ZoneOffset.UTC); // adjust timezone to UTC

                    Object convertedDateTime; // This will store either a Date or a Timestamp
                    if (zonedDateTime.toLocalTime().toSecondOfDay() == 0) {
                        // Represents the start of the day in UTC
                        convertedDateTime = Date.valueOf(zonedDateTime.toLocalDate()); // Convert to java.sql.Date with LocalDate
                    } else {
                        // Does not represent the start of the day in UTC
                        convertedDateTime = Timestamp.from(zonedDateTime.toInstant()); // Convert to Timestamp
                    }

                    input = input.replace(
                            STRING_MARKER + extractedStrings.get(i) + STRING_MARKER,
                            PARAM_MARKER + i + PARAM_MARKER
                    );
                    extractedStrings.set(i, convertedDateTime);
                } catch (Exception ignored) {
                    input = input.replace(
                            STRING_MARKER + extractedStrings.get(i) + STRING_MARKER,
                            PARAM_MARKER + i + PARAM_MARKER
                    );
                }
            } else if (((String) extractedStrings.get(i)).matches("\\d{4}-\\d{2}-\\d{2}")) {  // Match YYYY-MM-DD pattern
                try {
                    Date date = Date.valueOf((String) extractedStrings.get(i));  // Convert to java.sql.Date
                    input = input.replace(
                            STRING_MARKER + extractedStrings.get(i) + STRING_MARKER,
                            PARAM_MARKER + i + PARAM_MARKER
                    );
                    extractedStrings.set(i, date);
                } catch (Exception ignored) {
                    input = input.replace(
                            STRING_MARKER + extractedStrings.get(i) + STRING_MARKER,
                            PARAM_MARKER + i + PARAM_MARKER
                    );
                }
            } else if (((String) extractedStrings.get(i)).startsWith("DATE::")) {
                try {
                    String dateStr = ((String) extractedStrings.get(i)).replace("DATE::", "");
                    Date date = Date.valueOf(dateStr);
                    input = input.replace(
                            STRING_MARKER + extractedStrings.get(i) + STRING_MARKER,
                            PARAM_MARKER + i + PARAM_MARKER
                    );
                    extractedStrings.set(i, date);
                } catch (Exception ignored) {
                    input = input.replace(
                            STRING_MARKER + extractedStrings.get(i) + STRING_MARKER,
                            PARAM_MARKER + i + PARAM_MARKER
                    );
                }
                // if it's not a date constant, we will convert it into string parameter
            } else if (((String) extractedStrings.get(i)).startsWith("TIMESTAMP::")) {
                String rfcDateString = ((String) extractedStrings.get(i)).replace("TIMESTAMP::", "");
                DateTimeFormatter RFC_1123_DATE_TIME = DateTimeFormatter.RFC_1123_DATE_TIME;
                DateTimeFormatter RFC_3339_DATE_TIME = new DateTimeFormatterBuilder()
                        .parseCaseInsensitive()
                        .appendPattern("yyyy-MM-dd'T'HH:mm:ss")
                        .optionalStart()
                        .appendPattern(".SSS")
                        .optionalEnd()
                        .appendPattern("XXX")
                        .toFormatter();
                try {
                    ZonedDateTime zonedDateTime = ZonedDateTime.parse(rfcDateString, RFC_1123_DATE_TIME);
                    Timestamp timestamp = Timestamp.from(zonedDateTime.toInstant());
                    input = input.replace(
                            STRING_MARKER + extractedStrings.get(i) + STRING_MARKER,
                            PARAM_MARKER + i + PARAM_MARKER
                    );
                    extractedStrings.set(i, timestamp);
                } catch (Exception ignored) {
                    try {
                        ZonedDateTime zonedDateTime = ZonedDateTime.parse(rfcDateString, RFC_3339_DATE_TIME);
                        Timestamp timestamp = Timestamp.from(zonedDateTime.toInstant());
                        input = input.replace(
                                STRING_MARKER + extractedStrings.get(i) + STRING_MARKER,
                                PARAM_MARKER + i + PARAM_MARKER
                        );
                        extractedStrings.set(i, timestamp);
                    } catch (Exception ignore) {
                        input = input.replace(
                                STRING_MARKER + extractedStrings.get(i) + STRING_MARKER,
                                PARAM_MARKER + i + PARAM_MARKER
                        );
                    }
                }
                // if it's not a date constant, we will convert it into string parameter
            } else if (((String) extractedStrings.get(i)).startsWith("STRING::")) {
                String rfcDateString = ((String) extractedStrings.get(i)).replace("STRING::", "");
                input = input.replace(
                        STRING_MARKER + extractedStrings.get(i) + STRING_MARKER,
                        PARAM_MARKER + i + PARAM_MARKER
                );
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
