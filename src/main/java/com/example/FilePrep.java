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

    static private final String ROW_SEPARATOR = "::";

    static private final String CELL_SEPARATOR = "\\|";

    static private String[] splitStringBySeparator(String row, String separator) {
        return StringUtils.split(row, separator);
    }

    static private PTypeFamily getTypeFamilyFromTextFile(PCollection<String>textFile) {
        return textFile.getTypeFamily();
    }

    // split file by rows (with separator)
    static public PTable<String, String> getFileAsPTable(PCollection<String> textFile,
                                                             final Integer columnK,
                                                             final Integer columnV) {
        PTypeFamily tf = getTypeFamilyFromTextFile(textFile);
        return textFile.parallelDo(new DoFn<String, Pair<String, String>>() {

            @Override
            public void process(String input, Emitter<Pair<String, String>> emitter) {
                String[] parts = splitStringBySeparator(input, ROW_SEPARATOR);
                // Pair.of returns a (k, v) pair
                emitter.emit(Pair.of(parts[columnK], parts[columnV]));
            }
        }, tf.tableOf(tf.strings(), tf.strings()));
    }

    // split by row and by tag
    static public PTable<String, String> getMovieFileAsPTable(PCollection<String> textFile,
                                                         final Integer columnK,
                                                         final Integer columnV) {
        PTypeFamily tf = getTypeFamilyFromTextFile(textFile);
        return textFile.parallelDo(new DoFn<String, Pair<String, String>>() {

            @Override
            public void process(String input, Emitter<Pair<String, String>> emitter) {
                String[] parts = splitStringBySeparator(input, ROW_SEPARATOR);
                for (String genre: splitStringBySeparator(parts[columnV], CELL_SEPARATOR)) {
                    // Pair.of returns a (k, v) pair
                    emitter.emit(Pair.of(parts[columnK], genre));
                }
            }
        }, tf.tableOf(tf.strings(), tf.strings()));
    }

    // return three columns
    static public PTable<String, Pair<String, String>> getRatingsFileAsPTable(PCollection<String> textFile,
                                                                              final Integer columnK,
                                                                              final Integer columnVFirst,
                                                                              final Integer columnVSecond) {
        PTypeFamily tf = getTypeFamilyFromTextFile(textFile);
        return textFile.parallelDo(new DoFn<String, Pair<String, Pair<String, String>>>() {

            @Override
            public void process(String input, Emitter<Pair<String, Pair<String, String>>> emitter) {
                String[] parts = splitStringBySeparator(input, ROW_SEPARATOR);
                    // Pair.of returns a (k, v) pair
                    emitter.emit(Pair.of(parts[columnK], Pair.of(parts[columnVFirst], parts[columnVSecond])));
                }
        }, tf.tableOf(tf.strings(), tf.pairs(tf.strings(), tf.strings())));
    }
}

