package com.example.utilities;

import com.sun.tools.javac.util.List;
import org.apache.crunch.PTable;
import org.apache.crunch.Pair;
import org.apache.crunch.impl.mem.MemPipeline;
import org.apache.crunch.lib.Sort;
import org.apache.crunch.types.writable.Writables;
import org.junit.Assert;
import org.junit.Test;

/**
 * Created by hagar on 12/26/16.
 */
public class ComputeXPerYTest {

    PTable<String, Pair<String, String>> countXPerY =  MemPipeline.typedTableOf(
            Writables.tableOf(Writables.strings(), Writables.pairs(Writables.strings(), Writables.strings())),
            List.of(
                    Pair.of("1", Pair.of("Toy Story (1995)", "toys")),
                    Pair.of("1", Pair.of("Toy Story (1995)", "toys")),
                    Pair.of("1", Pair.of("Toy Story (1995)", "children")),
                    Pair.of("1", Pair.of("Toy Story (1995)", "hanks")),
                    Pair.of("2", Pair.of("Jumanji (1995)", "children")),
                    Pair.of("2", Pair.of("Jumanji (1995)", "game")),
                    Pair.of("3", Pair.of("Grumpier Old Men (1995)", "old men")),
                    Pair.of("3", Pair.of("Grumpier Old Men (1995)", "old men"))
            )
    );

    private PTable<Pair<String, String>, Long> countXPerYExpected = MemPipeline.typedTableOf(
            Writables.tableOf(Writables.pairs(Writables.strings(), Writables.strings()), Writables.longs()),
            List.of(
                    Pair.of(Pair.of("Toy Story (1995)", "toys"), 2L),
                    Pair.of(Pair.of("Toy Story (1995)", "children"), 1L),
                    Pair.of(Pair.of("Toy Story (1995)", "hanks"), 1L),
                    Pair.of(Pair.of("Jumanji (1995)", "children"), 1L),
                    Pair.of(Pair.of("Jumanji (1995)", "game"), 1L),
                    Pair.of(Pair.of("Grumpier Old Men (1995)", "old men"), 2L)
            )
    );

    private PTable<Pair<String, String>, Long> maxXPerY = countXPerYExpected;

    private PTable<String, String> maxXPerYExpected = MemPipeline.typedTableOf(
            Writables.tableOf(Writables.strings(), Writables.strings()),
            List.of(
                    Pair.of("Toy Story (1995)", "toys"),
                    Pair.of("Jumanji (1995)", "children|game"),
                    Pair.of("Grumpier Old Men (1995)", "old men")
            )
    );

    @Test
    public void testCountXPerY() {
        PTable<Pair<String, String>, Long> countXPerYObserved = ComputeXPerY.countXPerY(countXPerY);
        Assert.assertEquals(Sort.sort(countXPerYExpected).toString(), Sort.sort(countXPerYObserved).toString());
    }

    @Test
    public void testMaxXPerY() {
        PTable<String, String> maxXPerYObserved = ComputeXPerY.maxXPerY(maxXPerY);
        Assert.assertEquals(Sort.sort(maxXPerYExpected).toString(), Sort.sort(maxXPerYObserved).toString());
    }

}
