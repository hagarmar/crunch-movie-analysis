package com.example.utilities;

import org.apache.crunch.DoFn;
import org.apache.crunch.Emitter;
import org.apache.crunch.Pair;

/**
 * Created by hagar on 12/25/16.
 */
public class LineSplitterForPair extends DoFn <String, Pair<String, Pair<String, String>>> {

    private Integer keyColumn, valueFirstColumn, valueSecondColumn;

    public LineSplitterForPair(Integer columnForKey,
                               Integer columnForValueFirst,
                               Integer columnForValueSecond) {

        this.keyColumn = columnForKey;
        this.valueFirstColumn = columnForValueFirst;
        this.valueSecondColumn = columnForValueSecond;
    }


    @Override
    public void process(String input, Emitter<Pair<String, Pair<String, String>>> emitter) {
        String[] parts = LineSplitter.splitStringBySeparator(input, LineSplitter.ROW_SEPARATOR);
        // Future improvements:
        // 1. Compare parts.length against an expected value
        // 2. Check if K, V columns exist in parts
        // Pair.of returns a (k, v) pair
        emitter.emit(Pair.of(parts[this.keyColumn],
                Pair.of(parts[this.valueFirstColumn], parts[this.valueSecondColumn])));
    }
}



