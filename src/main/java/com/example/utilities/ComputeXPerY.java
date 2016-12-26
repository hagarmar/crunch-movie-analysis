package com.example.utilities;

import org.apache.crunch.MapFn;
import org.apache.crunch.PTable;
import org.apache.crunch.Pair;
import org.apache.crunch.lib.Aggregate;
import org.apache.crunch.types.writable.Writables;

/**
 * Several functions that allow
 * 1. counting how many Xs Per Y
 * 2. finding the argmax from the count of Xs Per Y
 * and ultimately returning the most frequent X per Y.
 * Created by hagar on 12/25/16.
 */
public class ComputeXPerY {

    static private PTable<Pair<String, String>, Long> countXPerY(
            PTable<String, Pair<String, String>> joinedXAndY) {
        PTable<String, String> countedXPerY = ReorderKV.makeValueIntoKey(joinedXAndY);
        // builtin function for returning the count per key
        return Aggregate.count(countedXPerY);
    }


    static private PTable<String, String> maxXPerY(PTable<Pair<String, String>, Long> countXPerY) {
        return ReorderKV.getReorderedTable(countXPerY) // returns PTable(String, (String, Integer))
                .groupByKey() // returns PTableGrouped(String, (String, Integer))
                .combineValues(new MostFrequentPair())
                // return just the argmax
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
