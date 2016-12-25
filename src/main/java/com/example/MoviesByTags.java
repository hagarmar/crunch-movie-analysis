package com.example;

import org.apache.crunch.*;
import org.apache.crunch.io.At;

/**
 * Created by hagar on 12/25/16.
 * Class for getting the most common tag per movie.
 */
public class MoviesByTags {

    static private PTable<String, String> prepMovies(PCollection<String> movies)  {
        return FilePrep.getFileAsPTable(movies, 0, 1);
    }


    static private PTable<String, String> prepTags(PCollection<String> tags)  {
        return FilePrep.getFileAsPTable(tags, 1, 2);
    }


    static private PTable<String, Pair<String, String>> joinMoviesAndTags(PTable<String, String> movies,
                                                                          PTable<String, String> tags) {
        return JoinAndFilter.compute(movies, tags);
    }


    static private void writeOutputFile (PTable<String, String> maxTagPerTitle, String outputPath) {
        maxTagPerTitle.write(At.textFile(outputPath), Target.WriteMode.OVERWRITE);
    }


    static public void run(PCollection<String> movies,
                          PCollection<String> tags,
                          String outputPath) {
        // Get PTable objects for each file
        // Parsing each line according to the separator "::"
        // Also choose which column will be the key column and which will be the value column
        PTable<String, String> movieClean =  prepMovies(movies);
        PTable<String, String> tagsClean = prepTags(tags);

        // Normalize data
        // 1. We may also want to consider using only lower case strings
        //    in case the data is not consistent.
        // 2. Remove null results after any left joins (see next code block)


        // Join movies (movie_id, movie_title) and tags (movie_id, tag) tables on movie id
        // Also filter out any null tags - movies that didn't have tags
        // We will end up with (movie_id, (movie_title, movie_tag))
        PTable<String, Pair<String, String>> joinedMovieTags = joinMoviesAndTags(movieClean, tagsClean);

        // Key on (movie_title, tag)
        // Set a value of 1 for each pair
        // groupByKey, aggregate.SUM_INTS
        // Now we don't need the movie_id anymore. Get to (user_id, genre)
        // count the appearance of each pair
        // Key on movie_title, groupByKey
        // Aggregation returns the (tag, count) with the highest count
        PTable<String, String> maxTagPerTitle = ComputeXPerY.countAndMaxXPerY(joinedMovieTags);

        // Write result to file
        writeOutputFile(maxTagPerTitle, outputPath);

    }
}