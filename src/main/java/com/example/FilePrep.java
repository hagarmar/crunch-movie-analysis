package com.example;

import org.apache.crunch.DoFn;
import org.apache.crunch.PTable;
import org.apache.crunch.Pair;
import org.apache.crunch.Emitter;
import org.apache.crunch.types.*;
import org.apache.crunch.PCollection;
import org.apache.commons.lang.StringUtils;


/**
 * Created by hagar on 12/12/16.
 */
public class FilePrep {

    // split file by rows (with separator)
    static public PTable<String, String> getFileAsPTable(PCollection<String> textFile,
                                                             final Integer columnK,
                                                             final Integer columnV) {
        PTypeFamily tf = textFile.getTypeFamily();
        return textFile.parallelDo(new DoFn<String, Pair<String, String>>() {

            @Override
            public void process(String input, Emitter<Pair<String, String>> emitter) {
                String[] parts = StringUtils.split(input, "::");
                // Pair.of returns a (k, v) pair
                emitter.emit(Pair.of(parts[columnK], parts[columnV]));
            }
        }, tf.tableOf(tf.strings(), tf.strings()));
    }

    // split by row and by tag
    static public PTable<String, String> getMovieFileAsPTable(PCollection<String> textFile,
                                                         final Integer columnK,
                                                         final Integer columnSplit) {
        PTypeFamily tf = textFile.getTypeFamily();
        return textFile.parallelDo(new DoFn<String, Pair<String, String>>() {

            @Override
            public void process(String input, Emitter<Pair<String, String>> emitter) {
                String[] parts = StringUtils.split(input, "::");
                for (String genre: parts[columnSplit].split("\\|")) {
                    // Pair.of returns a (k, v) pair
                    emitter.emit(Pair.of(parts[columnK], genre));
                }
            }
        }, tf.tableOf(tf.strings(), tf.strings()));
    }


}

