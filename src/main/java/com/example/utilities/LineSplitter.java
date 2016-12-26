package com.example.utilities;

import org.apache.commons.lang.StringUtils;
import org.apache.crunch.*;

/**
 * Splits a textfile line by separator.
 * Inputs: key column for pair, value column for pair,
 * number of expected columns in the row.
 * Emits a pair based on the k, v columns supplied.
 * Created by hagar on 12/25/16.
 */
public class LineSplitter extends DoFn<String, Pair<String, String>> {

    public Integer keyColumn, valueColumn, numExpectedRows;

    static String ROW_SEPARATOR = "::";;

    static public Integer numExpectedRowsMovies = 3;

    static public Integer numExpectedRowsTags = 4;

    public LineSplitter(Integer columnForKey,
                        Integer columnForValue,
                        Integer numExpectedRows) {

        this.keyColumn = columnForKey;
        this.valueColumn = columnForValue;
        this.numExpectedRows = numExpectedRows;
    }

    static public String[] splitStringBySeparator(String row, String separator) {
        return StringUtils.split(row, separator);
    }

    @Override
    public void process (String input, Emitter <Pair<String, String >> emitter) {
        String[] parts = splitStringBySeparator(input, ROW_SEPARATOR);
        if (parts.length == this.numExpectedRows) {
            // Pair.of returns a (k, v) pair
            emitter.emit(Pair.of(parts[this.keyColumn], parts[this.valueColumn]));
        }
    }
}

