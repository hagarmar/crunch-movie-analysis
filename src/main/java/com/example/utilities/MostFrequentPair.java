package com.example.utilities;

import org.apache.crunch.Emitter;
import org.apache.crunch.Pair;
import org.apache.crunch.CombineFn;

/**
 * Gets the results of a groupByKey and returns the most
 * frequent pair in the iterable.
 * If several values appear most frequently, both will be returned.
 * Created by hagar on 12/25/16.
 */
public class MostFrequentPair extends CombineFn<String, Pair<String, Long>>  {
    private String SEPARATOR = "|";

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
            if ((dw.second() == maxValue) & !(dw.first().equals(maxTag))) {
                maxTag = maxTag + SEPARATOR + dw.first();
            }
        }
        emitter.emit(Pair.of(input.first(), Pair.of(maxTag, maxValue)));
    }

}
