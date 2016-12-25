package com.example;

import org.apache.crunch.FilterFn;
import org.apache.crunch.PTable;
import org.apache.crunch.Pair;
import org.apache.crunch.lib.Join;

/**
 * Created by hagar on 12/25/16.
 */
public class JoinAndFilter {

    static public PTable<String, Pair<String, String>> compute(PTable<String, String> leftTable,
                                                                PTable<String, String> rightTable) {
        return Join
                .leftJoin(leftTable, rightTable)
                .filter(new FilterFn<Pair<String, Pair<String, String>>>() {

                    @Override
                    public boolean accept(final Pair<String, Pair<String, String>> joinedTable) {
                        if (joinedTable.second().second() == null) {
                            return false;
                        } else {
                            return true;
                        }
                    }
                });
    }

}

