package com.example.utilities;

import org.apache.crunch.FilterFn;
import org.apache.crunch.Pair;

/**
 * Filters out rows that don't have ratings.
 *
 * Created by hagar on 12/25/16.
 */
public class FilterMissingRatings extends FilterFn<Pair<String, Pair<String, String>>> {

    private Integer lengthRating = 1;

    @Override
    public boolean accept(Pair<String, Pair<String, String>> pairWithPairValue) {
        return (pairWithPairValue.second().second().length() == lengthRating);
    }
}
