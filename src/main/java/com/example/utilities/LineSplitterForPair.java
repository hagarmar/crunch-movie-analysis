package com.example.utilities;

import org.apache.crunch.DoFn;
import org.apache.crunch.Emitter;
import org.apache.crunch.Pair;

/**
 * Created by hagar on 12/25/16.
 */
public class LineSplitterForPair extends DoFn <String, Pair<String, Pair<String, String>>> {

    private Integer keyColumn, valueFirstColumn, valueSecondColumn, numExpectedRows;

    static public Integer numExpectedRowsRatings = 4;

    public LineSplitterForPair(Integer columnForKey,
                               Integer columnForValueFirst,
                               Integer columnForValueSecond,
                               Integer numExpectedRows) {

        this.keyColumn = columnForKey;
        this.valueFirstColumn = columnForValueFirst;
        this.valueSecondColumn = columnForValueSecond;
        this.numExpectedRows = numExpectedRows;
    }


    @Override
    public void process(String input, Emitter<Pair<String, Pair<String, String>>> emitter) {
        String[] parts = LineSplitter.splitStringBySeparator(input, LineSplitter.ROW_SEPARATOR);
        // Pair.of returns a (k, v) pair
        if (parts.length == numExpectedRows) {
            emitter.emit(Pair.of(parts[this.keyColumn],
                    Pair.of(parts[this.valueFirstColumn], parts[this.valueSecondColumn])));
        }
    }
}



