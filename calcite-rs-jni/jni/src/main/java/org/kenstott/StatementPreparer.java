package org.kenstott;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.LocalDate;
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
        ArrayList<String> extractedStrings = extractMarkedUpStrings(input);
        String modifiedInput = replaceWithIndexedQuestionMarks(input, extractedStrings);
        ArrayList<Object> extractedStringParams = new ArrayList<>();
        modifiedInput = findParams(modifiedInput, extractedStrings, extractedStringParams);
        PreparedStatement preparedStatement = connection.prepareStatement(modifiedInput);

        // Regex pattern for RFC3339
        String datePattern = "^[1-9]\\d{3}-(?:(?:0[1-9]|1[0-3])-(?:0[1-9]|1[0-9]|2[0-2])|(?:0[13-9]|1[0-2])-30|(?:0[13578]|1[02])-31)T(?:[01]\\d|2[0-3]):[0-5]\\d:[0-5]\\d(?:\\.\\d+)?(?:Z|[+-][01]\\d:[0-5]\\d)$";
        Pattern pattern = Pattern.compile(datePattern);

        for (int i = 0; i < extractedStringParams.size(); i++) {
            Object item = extractedStringParams.get(i);
            if (item instanceof String) {
                String strItem = (String) item;
                if (pattern.matcher(strItem).matches()) {
                    // this string can be parsed as a date
                    preparedStatement.setDate(i + 1, java.sql.Date.valueOf(LocalDate.parse(strItem, DateTimeFormatter.ISO_OFFSET_DATE_TIME)));
                } else {
                    preparedStatement.setString(i + 1, strItem);
                }
            }
        }
        return preparedStatement;
    }

    private static String findParams(String input, ArrayList<String> extractedStrings, ArrayList<Object> params) {
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

    private static ArrayList<String> extractMarkedUpStrings(String input) {
        String marker = STRING_MARKER;
        ArrayList<String> extractedList = new ArrayList<>();

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

    private static String replaceWithIndexedQuestionMarks(String input, ArrayList<String> extractedStrings) {
        Pattern pattern = Pattern.compile("^\\d{4}-\\d{1,2}-\\d{1,2}([T\\s]\\d{2}:\\d{2}:\\d{2}(\\.\\d+)?)?([Zz]|([+\\-]\\d{2}:\\d{2}))?$");

        for (int i = 0; i < extractedStrings.size(); ++i) {
            Matcher matcher = pattern.matcher(extractedStrings.get(i));
            if (matcher.matches()) {
                input = input.replace(
                        STRING_MARKER + extractedStrings.get(i) + STRING_MARKER,
                        "'"+ extractedStrings.get(i) + "'"
                );
                extractedStrings.remove(i);
                --i;
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
