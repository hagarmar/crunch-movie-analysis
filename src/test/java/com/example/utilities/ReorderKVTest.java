package com.example.utilities;

import com.sun.tools.javac.util.List;
import org.apache.crunch.PTable;
import org.apache.crunch.Pair;
import org.apache.crunch.impl.mem.MemPipeline;
import org.apache.crunch.types.writable.Writables;
import org.junit.Assert;
import org.junit.Test;

/**
 * Created by hagar on 12/26/16.
 */
public class ReorderKVTest {

    private PTable<Pair<String, String>, Long> reorderedTable = MemPipeline.typedTableOf(
            Writables.tableOf(Writables.pairs(Writables.strings(), Writables.strings()), Writables.longs()),
            List.of(
                    Pair.of(Pair.of("1", "toys"), 2L),
                    Pair.of(Pair.of("1", "children"), 1L),
                    Pair.of(Pair.of("1", "hanks"), 4L)
            )
    );

    private PTable<String, Pair<String, Long>> reorderedTableExpected = MemPipeline.typedTableOf(
            Writables.tableOf(Writables.strings(), Writables.pairs(Writables.strings(), Writables.longs())),
            List.of(
                    Pair.of("1", Pair.of("toys", 2L)),
                    Pair.of("1", Pair.of("children", 1L)),
                    Pair.of("1", Pair.of("hanks", 4L))
            )
    );

    private PTable<String, Pair<String, String>> makeValueIntoKey = MemPipeline.typedTableOf(
            Writables.tableOf(Writables.strings(), Writables.pairs(Writables.strings(), Writables.strings())),
            List.of(
                    Pair.of("1", Pair.of("Toy Story (1995)", "toys")),
                    Pair.of("1", Pair.of("Toy Story (1995)", "children")),
                    Pair.of("1", Pair.of("Toy Story (1995)", "hanks"))
            )
    );

    private PTable<String, String> makeValueIntoKeyExpected = MemPipeline.typedTableOf(
            Writables.tableOf(Writables.strings(), Writables.strings()),
            List.of(
                    Pair.of("Toy Story (1995)", "toys"),
                    Pair.of("Toy Story (1995)", "children"),
                    Pair.of("Toy Story (1995)", "hanks")
            )
    );



    @Test
    public void testReorderedTable() {
        PTable<String, Pair<String, Long>> reorderedTableObserved = ReorderKV.getReorderedTable(reorderedTable);
        Assert.assertEquals(reorderedTableExpected.toString(), reorderedTableObserved.toString());
    }


    @Test
    public void testMakeValueIntoKey() {
        PTable<String, String> makeValueIntoKeyObserved = ReorderKV.makeValueIntoKey(makeValueIntoKey);
        Assert.assertEquals(makeValueIntoKeyExpected.toString(), makeValueIntoKeyObserved.toString());
    }
}
