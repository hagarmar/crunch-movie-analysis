package com.example;

import com.example.utilities.ComputeXPerY;
import com.example.utilities.FilePrep;
import org.apache.crunch.*;
import org.apache.crunch.io.At;
import org.apache.crunch.lib.Join;

/**
 * Created by hagar on 12/25/16.
 * Class for getting the most common tag per movie.
 */
public class MoviesByTags {

    static public PTable<String, String> prepMovies(PCollection<String> movies)  {
        return FilePrep.getFileAsPTable(movies, 0, 1);
    }


    static public PTable<String, String> prepTags(PCollection<String> tags)  {
        return FilePrep.getFileAsPTable(tags, 1, 2);
    }


    static public PTable<String, Pair<String, String>> joinMoviesAndTags(PTable<String, String> movies,
                                                                          PTable<String, String> tags) {
        return Join.innerJoin(movies, tags);
    }


    static private void writeOutputFile (PTable<String, String> maxTagPerTitle, String outputPath) {
        maxTagPerTitle.write(At.textFile(outputPath), Target.WriteMode.OVERWRITE);
    }


    static public PTable<String, String> getMaxTagPerTitle(PCollection<String> movies,
                                                           PCollection<String> tags) {
        // prep the raw data for compute
        PTable<String, String> movieClean = prepMovies(movies);
        PTable<String, String> tagsClean = prepTags(tags);

        // Normalize data
        // We may want to consider using only lower case strings in case the data is not consistent
        // Or performing all operations on IDs.
        // Here we may also want to drop duplicates: tags applied by the same user for the same movie

        // Join movies (movie_id, movie_title) and tags (movie_id, tag) tables on movie id
        // We will end up with (movie_id, (movie_title, movie_tag))
        PTable<String, Pair<String, String>> joinedMovieTags = joinMoviesAndTags(movieClean, tagsClean);

        // count number of tags per movie_id
        // return the most common tag
        PTable<String, String> maxTagPerTitle = ComputeXPerY.countAndMaxXPerY(joinedMovieTags);

        return maxTagPerTitle;
    }


    static public void run(PCollection<String> movies,
                          PCollection<String> tags,
                          String outputPath) {

        // Write result to file
        writeOutputFile(getMaxTagPerTitle(movies, tags), outputPath);

    }
}