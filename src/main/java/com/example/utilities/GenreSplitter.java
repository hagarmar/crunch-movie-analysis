package com.example.utilities;

import org.apache.commons.lang.StringUtils;
import org.apache.crunch.DoFn;
import org.apache.crunch.Pair;
import org.apache.crunch.Emitter;

/**
 * Splits the "genre" cell of a movie PTable.
 * Created by hagar on 12/25/16.
 */
public class GenreSplitter extends DoFn<Pair<String, String>, Pair<String, String>> {

    String GENRE_SEPARATOR = "\\|";

    @Override
    public void process (Pair<String, String> row, Emitter <Pair<String, String >> emitter) {
        String[] genres =  LineSplitter.splitStringBySeparator(row.second(), GENRE_SEPARATOR);
        for (String movieGenre: genres) {
            emitter.emit(Pair.of(row.first(), movieGenre));
        }
    }

}
