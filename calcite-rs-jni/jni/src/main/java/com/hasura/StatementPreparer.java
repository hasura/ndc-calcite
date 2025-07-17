package com.hasura;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Date;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;
import java.util.ArrayList;
import java.util.HashMap;
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
    public static PreparedStatement prepare(String input, Connection connection, Boolean convertDates) throws SQLException {
        ArrayList<Object> extractedStrings = extractMarkedUpStrings(input);
        String modifiedInput = replaceWithIndexedQuestionMarks(input, extractedStrings, convertDates);
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
    private static String replaceWithIndexedQuestionMarks(String input, ArrayList<Object> extractedStrings, Boolean convertDates) {
        // Handle all RFC 3339 / ISO 8601 timestamp formats
        // UTC with Z - with optional seconds and variable fractional seconds
        Pattern utcZPattern = Pattern.compile("^\\d{4}-\\d{1,2}-\\d{1,2}[T\\s]\\d{1,2}:\\d{2}(:\\d{2})?(\\.\\d{1,9})?Z$");

        // Timezone offset with colon (+05:00, -08:00)
        Pattern timezoneOffsetPattern = Pattern.compile("^\\d{4}-\\d{1,2}-\\d{1,2}T\\d{1,2}:\\d{2}(:\\d{2})?(\\.\\d{1,9})?[+-]\\d{2}:\\d{2}$");

        // Timezone offset without colon (+0500, -0800)
        Pattern timezoneOffsetNoColonPattern = Pattern.compile("^\\d{4}-\\d{1,2}-\\d{1,2}T\\d{1,2}:\\d{2}(:\\d{2})?(\\.\\d{1,9})?[+-]\\d{4}$");

        // Date only
        Pattern dateOnlyPattern = Pattern.compile("^\\d{4}-\\d{2}-\\d{2}$");

        // Optional: Date + Time without timezone (assumes local time)
        Pattern localDateTimePattern = Pattern.compile("^\\d{4}-\\d{1,2}-\\d{1,2}[T\\s]\\d{1,2}:\\d{2}(:\\d{2})?(\\.\\d{1,9})?$");

        DateTimeFormatter rfcFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSX");
        DateTimeFormatter flexibleFormatter = new DateTimeFormatterBuilder()
                .parseCaseInsensitive()
                .appendPattern("yyyy-MM-dd")
                .appendLiteral('T')
                .appendPattern("HH:mm")
                .optionalStart()
                .appendLiteral(':')
                .appendPattern("ss")
                .optionalEnd()
                .optionalStart()
                .appendFraction(ChronoField.NANO_OF_SECOND, 1, 9, true)
                .optionalEnd()
                .optionalStart()
                .appendPattern("XXX")  // Handles +05:00, Z
                .optionalEnd()
                .optionalStart()
                .appendPattern("XX")   // Handles +0500
                .optionalEnd()
                .toFormatter();

        for (int i = 0; i < extractedStrings.size(); ++i) {
            String currentString = (String) extractedStrings.get(i);

            if (convertDates) {
                Matcher utcMatcher = utcZPattern.matcher(currentString);
                Matcher offsetMatcher = timezoneOffsetPattern.matcher(currentString);
                Matcher offsetNoColonMatcher = timezoneOffsetNoColonPattern.matcher(currentString);
                Matcher dateOnlyMatcher = dateOnlyPattern.matcher(currentString);
                Matcher localDateTimeMatcher = localDateTimePattern.matcher(currentString);

                if (utcMatcher.matches() || offsetMatcher.matches() || offsetNoColonMatcher.matches()) {
                    // Handle any timestamp with timezone information
                    try {
                        ZonedDateTime zonedDateTime = ZonedDateTime.parse(currentString, flexibleFormatter);
                        zonedDateTime = zonedDateTime.withZoneSameInstant(ZoneOffset.UTC);

                        Object convertedDateTime = createDateTimeObject(zonedDateTime, currentString);

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
                } else if (localDateTimeMatcher.matches()) {
                    // Handle local datetime (no timezone) - assume UTC
                    try {
                        LocalDateTime localDateTime = LocalDateTime.parse(currentString, flexibleFormatter);
                        ZonedDateTime zonedDateTime = localDateTime.atZone(ZoneOffset.UTC);

                        Object convertedDateTime = createDateTimeObject(zonedDateTime, currentString);

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
                } else if (dateOnlyMatcher.matches()) {
                    // Handle date-only format: YYYY-MM-DD
                    try {
                        Date date = Date.valueOf(currentString);
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
                } else if (currentString.startsWith("DATE::")) {
                    // Handle DATE:: prefix
                    try {
                        String dateStr = currentString.replace("DATE::", "");
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
                } else if (currentString.startsWith("TIMESTAMP::")) {
                    // Handle TIMESTAMP:: prefix (existing logic)
                    String rfcDateString = currentString.replace("TIMESTAMP::", "");
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
                } else if (currentString.startsWith("STRING::")) {
                    // Handle STRING:: prefix
                    input = input.replace(
                            STRING_MARKER + extractedStrings.get(i) + STRING_MARKER,
                            PARAM_MARKER + i + PARAM_MARKER
                    );
                } else {
                    // Default case - no conversion
                    input = input.replace(
                            STRING_MARKER + extractedStrings.get(i) + STRING_MARKER,
                            PARAM_MARKER + i + PARAM_MARKER
                    );
                }
            } else {
                // convertDates is false - no conversion
                input = input.replace(
                        STRING_MARKER + extractedStrings.get(i) + STRING_MARKER,
                        PARAM_MARKER + i + PARAM_MARKER
                );
            }
        }
        return input;
    }

    /**
     * Helper method to create appropriate DateTime object
     * If the original string contains time components (T and time), always create Timestamp
     * Only create Date for date-only strings (YYYY-MM-DD)
     */
    private static Object createDateTimeObject(ZonedDateTime zonedDateTime, String originalString) {
        // If the original string contains 'T' (time separator), it's a timestamp regardless of the time values
        if (originalString.contains("T")) {
            return Timestamp.from(zonedDateTime.toInstant());
        } else {
            // Only for pure date strings like "2025-07-10" should we return Date
            return Date.valueOf(zonedDateTime.toLocalDate());
        }
    }
}
