package org.kenstott;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

class StatementPreparer {

    static String STRING_MARKER = "__UTF8__";
    static String PARAM_MARKER = "?";

    public static PreparedStatement prepare(String input, Connection connection) throws SQLException {
        String[] extractedStrings = extractMarkedUpStrings(input);
        String modifiedInput = replaceWithIndexedQuestionMarks(input, extractedStrings);
        ArrayList<Object> extractedStringParams = new ArrayList<>();
        modifiedInput = findParams(modifiedInput, extractedStrings, extractedStringParams);
        PreparedStatement preparedStatement = connection.prepareStatement(modifiedInput);
        for (int i = 0; i < extractedStringParams.size(); i++) {
            Object item = extractedStringParams.get(i);
            if (item instanceof String) {
                preparedStatement.setString(i + 1, (String) item);
            }
        }
        return preparedStatement;
    }

    private static String findParams(String input, String[] extractedStrings, ArrayList<Object> params) {
        Pattern pattern = Pattern.compile("\\?(\\d+)\\?");
        Matcher matcher = pattern.matcher(input);
        return matcher.replaceAll(match -> {
            String value = match.group().replaceAll("\\?", "");
            int index = Integer.parseInt(value);
            params.add(extractedStrings[index]);
            return PARAM_MARKER;
        });
    }

    private static String[] extractMarkedUpStrings(String input) {
        String marker = STRING_MARKER;
        List<String> extractedList = new ArrayList<>();

        int startIndex = input.indexOf(marker);
        while (startIndex != -1) {
            int endIndex = input.indexOf(marker, startIndex + marker.length());
            if (endIndex != -1) {
                String extracted = input.substring(startIndex + marker.length(), endIndex);
                extractedList.add(extracted);
            }
            startIndex = input.indexOf(marker, endIndex + marker.length());
        }

        return extractedList.toArray(new String[0]);
    }

    public static String replaceWithIndexedQuestionMarks(String input, String[] extractedStrings) {
        for (int i = 0; i < extractedStrings.length; i++) {
            input = input.replace(
                    STRING_MARKER + extractedStrings[i] + STRING_MARKER,
                    PARAM_MARKER + i + PARAM_MARKER
            );
        }
        return input;
    }
}
