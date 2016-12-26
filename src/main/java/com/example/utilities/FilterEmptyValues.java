package com.example.utilities;

import org.apache.crunch.FilterFn;
import org.apache.crunch.Pair;

/**
 * Filters out pairs whose second value is null.
 *
 * Created by hagar on 12/25/16.
 */
public class FilterEmptyValues extends FilterFn<Pair<String, Pair<String, String>>> {

    @Override
    public boolean accept(Pair<String, Pair<String, String>> pairWithPairValue) {
        return !(pairWithPairValue.second().second() == "");
    }
}
