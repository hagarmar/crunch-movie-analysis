package com.example.utilities;

import com.sun.tools.javac.util.List;
import org.apache.crunch.PTable;
import org.apache.crunch.Pair;
import org.apache.crunch.impl.mem.MemPipeline;
import org.apache.crunch.types.writable.Writables;
import org.junit.Assert;
import org.junit.Test;

/**
 * Created by hagar on 12/25/16.
 */

public class MostFrequentPairTest {

    private PTable<String, Pair<String, Long>> caseOneMaxFirst = MemPipeline.typedTableOf(
            Writables.tableOf(Writables.strings(), Writables.pairs(Writables.strings(), Writables.longs())),
            List.of(
                    Pair.of("1", Pair.of("toys", 2L)),
                    Pair.of("1", Pair.of("children", 1L)),
                    Pair.of("1", Pair.of("hanks", 4L))
            )
    );

    private PTable<String, Pair<String, Long>> caseOneMaxFirstExpected = MemPipeline.typedTableOf(
            Writables.tableOf(Writables.strings(), Writables.pairs(Writables.strings(), Writables.longs())),
            List.of(
                    Pair.of("1", Pair.of("hanks", 4L))
            )
    );


    private PTable<String, Pair<String, Long>> caseOneMaxLast = MemPipeline.typedTableOf(
            Writables.tableOf(Writables.strings(), Writables.pairs(Writables.strings(), Writables.longs())),
            List.of(
                    Pair.of("4", Pair.of("toys", 4L)),
                    Pair.of("4", Pair.of("children", 1L)),
                    Pair.of("4", Pair.of("hanks", 2L))
                    )
    );

    private PTable<String, Pair<String, Long>> caseOneMaxLastExpected = MemPipeline.typedTableOf(
            Writables.tableOf(Writables.strings(), Writables.pairs(Writables.strings(), Writables.longs())),
            List.of(
                    Pair.of("4", Pair.of("toys", 4L))
            )
    );

    private PTable<String, Pair<String, Long>> caseOne = MemPipeline.typedTableOf(
            Writables.tableOf(Writables.strings(), Writables.pairs(Writables.strings(), Writables.longs())),
            List.of(
                    Pair.of("4", Pair.of("toys", 4L))
            )
    );

    private PTable<String, Pair<String, Long>> caseOneExpected = MemPipeline.typedTableOf(
            Writables.tableOf(Writables.strings(), Writables.pairs(Writables.strings(), Writables.longs())),
            List.of(
                    Pair.of("4", Pair.of("toys", 4L))
            )
    );


    private PTable<String, Pair<String, Long>> caseJustTie = MemPipeline.typedTableOf(
            Writables.tableOf(Writables.strings(), Writables.pairs(Writables.strings(), Writables.longs())),
            List.of(
                    Pair.of("2", Pair.of("game", 1L)),
                    Pair.of("2", Pair.of("children", 1L))
            )
    );

    private PTable<String, Pair<String, Long>> caseJustTieExpected = MemPipeline.typedTableOf(
            Writables.tableOf(Writables.strings(), Writables.pairs(Writables.strings(), Writables.longs())),
            List.of(
                    Pair.of("2", Pair.of("game|children", 1L))
            )
    );


    private PTable<String, Pair<String, Long>> caseTieIsMax = MemPipeline.typedTableOf(
            Writables.tableOf(Writables.strings(), Writables.pairs(Writables.strings(), Writables.longs())),
            List.of(
                    Pair.of("2", Pair.of("game", 2L)),
                    Pair.of("2", Pair.of("children", 2L)),
                    Pair.of("2", Pair.of("not good", 1L))
            )
    );

    private PTable<String, Pair<String, Long>> caseTieIsMaxExpected = MemPipeline.typedTableOf(
            Writables.tableOf(Writables.strings(), Writables.pairs(Writables.strings(), Writables.longs())),
            List.of(
                    Pair.of("2", Pair.of("game|children", 2L))
            )
    );

    private PTable<String, Pair<String, Long>> caseTieNotMax = MemPipeline.typedTableOf(
            Writables.tableOf(Writables.strings(), Writables.pairs(Writables.strings(), Writables.longs())),
            List.of(
                    Pair.of("2", Pair.of("game", 2L)),
                    Pair.of("2", Pair.of("children", 2L)),
                    Pair.of("2", Pair.of("good", 4L))
            )
    );

    private PTable<String, Pair<String, Long>> caseTieNotMaxExpected = MemPipeline.typedTableOf(
            Writables.tableOf(Writables.strings(), Writables.pairs(Writables.strings(), Writables.longs())),
            List.of(
                    Pair.of("2", Pair.of("good", 4L))
            )
    );


    @Test
    public void testOneMaxFirst() {
        PTable<String, Pair<String, Long>>mostFrequentExpected = caseOneMaxFirst.groupByKey().combineValues(new MostFrequentPair());

        Assert.assertEquals(mostFrequentExpected, caseOneMaxFirstExpected);
    }

    @Test
    public void testOneMaxLast() {
        PTable<String, Pair<String, Long>>mostFrequentExpected = caseOneMaxLast.groupByKey().combineValues(new MostFrequentPair());

        Assert.assertEquals(mostFrequentExpected, caseOneMaxLastExpected);
    }

    @Test
    public void testOne() {
        PTable<String, Pair<String, Long>>mostFrequentExpected = caseOne.groupByKey().combineValues(new MostFrequentPair());

        Assert.assertEquals(mostFrequentExpected, caseOneExpected);
    }


    @Test
    public void testJustTie() {
        PTable<String, Pair<String, Long>>mostFrequentExpected = caseJustTie.groupByKey().combineValues(new MostFrequentPair());

        Assert.assertEquals(mostFrequentExpected, caseJustTieExpected);
    }

    @Test
    public void testTieIsMax() {
        PTable<String, Pair<String, Long>>mostFrequentExpected = caseTieIsMax.groupByKey().combineValues(new MostFrequentPair());

        Assert.assertEquals(mostFrequentExpected, caseTieIsMaxExpected);
    }

    @Test
    public void testTieNotMax() {
        PTable<String, Pair<String, Long>>mostFrequentExpected = caseTieNotMax.groupByKey().combineValues(new MostFrequentPair());

        Assert.assertEquals(mostFrequentExpected, caseTieNotMaxExpected);
    }
}
