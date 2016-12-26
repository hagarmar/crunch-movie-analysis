package com.example.utilities;

import static org.junit.Assert.*;
import org.apache.crunch.FilterFn;
import org.apache.crunch.Pair;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.is;

/**
 * Created by hagar on 12/25/16.
 */
public class FilterEmptyValuesTest {

    @Test
    public void filterEmptyValuesTest() {
        FilterFn<Pair<String, Pair<String, String>>> filter = new FilterEmptyValues();
        assertThat(filter.accept(Pair.of("1", Pair.of("a", "a"))), is(true));
        assertThat(filter.accept(Pair.of("1", Pair.of("a", ""))), is(false));
    }

}
