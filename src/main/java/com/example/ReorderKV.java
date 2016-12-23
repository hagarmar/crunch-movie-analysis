package com.example;

import org.apache.crunch.*;
import org.apache.crunch.DoFn;
import org.apache.crunch.types.writable.Writables;

/**
 * Created by hagar on 12/16/16.
 */

public class ReorderKV {

    static public PTable<String, Pair<String, Integer>> getReorderedTable(
            PTable<Pair<String, String>, Integer> oldData) {
        return oldData.parallelDo(new DoFn<Pair<Pair<String, String>, Integer>, Pair<String, Pair<String, Integer>>>() {
            @Override
            public void process(Pair<Pair<String, String>, Integer> input,
                                Emitter<Pair<String, Pair<String, Integer>>> emitter) {
                // Pair.of returns a (k, v) pair
                emitter.emit(Pair.of(input.first().first(), Pair.of(input.first().second(), input.second())));
            }
        }, Writables.tableOf(Writables.strings(), Writables.pairs(Writables.strings(), Writables.ints())));
    }
}
