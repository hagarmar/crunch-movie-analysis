package com.example;

import org.apache.crunch.CombineFn;
import org.apache.crunch.MapFn;
import org.apache.crunch.PTable;
import org.apache.crunch.Pair;
import org.apache.crunch.lib.Aggregate;
import org.apache.crunch.types.writable.Writables;
import org.apache.crunch.Emitter;

/**
 * Created by hagar on 12/25/16.
 */
public class ComputeXPerY {

    static private PTable<Pair<String, String>, Long> countXPerY(
            PTable<String, Pair<String, String>> joinedXAndY) {
        PTable<String, String> countedXPerY = ReorderKV.makeValueIntoKey(joinedXAndY);
        return Aggregate.count(countedXPerY);
    }


    static private PTable<String, String> maxXPerY(PTable<Pair<String, String>, Long> countXPerY) {
        return ReorderKV.getReorderedTable(countXPerY) // returns PTable(String, (String, Integer))
                .groupByKey() // returns PTableGrouped(String, (String, Integer))
                .combineValues(new CombineFn<String, Pair<String, Long>>() {
                    @Override
                    public void process(Pair<String, Iterable<Pair<String, Long>>> input,
                                        Emitter<Pair<String, Pair<String, Long>>> emitter) {
                        String maxTag = null;
                        Long maxValue = 0L;
                        for (Pair<String, Long> dw : input.second()) {
                            if (dw.second() > maxValue) {
                                maxValue = dw.second();
                                maxTag = dw.first();
                            }
                        }
                        emitter.emit(Pair.of(input.first(), Pair.of(maxTag, maxValue)));
                    }
                })
                .mapValues(new MapFn<Pair<String, Long>, String>() {
                    public String map(Pair<String, Long> maxTagPair) {
                        return maxTagPair.first();
                    }
                }, (Writables.strings()));

    }

    static public PTable<String, String> countAndMaxXPerY(PTable<String, Pair<String, String>> joinedXAndY) {
        PTable<Pair<String, String>, Long> counted = countXPerY(joinedXAndY);
        return maxXPerY(counted);
    }
}
