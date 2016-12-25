package com.example;

import org.apache.crunch.*;
import org.apache.crunch.DoFn;
import org.apache.crunch.types.writable.Writables;

/**
 * Created by hagar on 12/16/16.
 */

public class ReorderKV {

    static public PTable<String, Pair<String, Long>> getReorderedTable(
            PTable<Pair<String, String>, Long> oldData) {
        return oldData.parallelDo(new DoFn<Pair<Pair<String, String>, Long>, Pair<String, Pair<String, Long>>>() {
            @Override
            public void process(Pair<Pair<String, String>, Long> input,
                                Emitter<Pair<String, Pair<String, Long>>> emitter) {
                // Pair.of returns a (k, v) pair
                emitter.emit(Pair.of(input.first().first(), Pair.of(input.first().second(), input.second())));
            }
        }, Writables.tableOf(Writables.strings(), Writables.pairs(Writables.strings(), Writables.longs())));
    }

    static public PTable<String, String> makeValueIntoKey(PTable<String, Pair<String, String>> oldData) {
        return oldData.parallelDo(new DoFn<Pair<String, Pair<String, String>>, Pair<String, String>>() {

            @Override
            public void process(Pair<String, Pair<String, String>> input,
                                Emitter<Pair<String, String>> emitter) {
                // Pair.of returns a (k, v) pair
                emitter.emit(Pair.of(input.second().first(), input.second().second()));
            }
        }, Writables.tableOf(Writables.strings(), Writables.strings()));
    }

}
