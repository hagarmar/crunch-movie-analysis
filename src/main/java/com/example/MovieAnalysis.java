package com.example;

import org.apache.crunch.*;
import org.apache.crunch.fn.Aggregators;
import org.apache.crunch.impl.mr.MRPipeline;
//import org.apache.crunch.impl.mem.MemPipeline;
import org.apache.crunch.io.At;
import org.apache.crunch.lib.Aggregate;
import org.apache.crunch.lib.Join;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.crunch.MapFn;
import org.apache.crunch.types.writable.Writables;
import org.apache.crunch.lib.PTables;

import java.io.Serializable;


/**
 * most common tag for a movie title, and the most common genre for a rater.
 */
public class MovieAnalysis extends Configured implements Tool, Serializable {

    // ToolRunner parses command line args for hadoop mr jobs
    // It makes them available to our program via getConf() (inherited from Configured)
    public static void main(String[] args) throws Exception {
        ToolRunner.run(new Configuration(), new MovieAnalysis(), args);
    }

    public int run(String[] args) throws Exception {

        if (args.length < 5) {
            System.err.println("Usage: hadoop jar crunchtask-1.0-SNAPSHOT-job.jar"
                    + " input");
            System.err.println();
            GenericOptionsParser.printGenericCommandUsage(System.err);
            return 1;
        }

        // String inputPathRates = args[0];
        String inputPathTags = args[0];
        String inputPathMovies = args[1];
        String inputPathRates = args[2];
        String outputPathTagsMovies = args[3];
        String outputPathUserGenres = args[4];

        // Creating the pipeline instance
        Pipeline pipeline = new MRPipeline(MovieAnalysis.class, getConf());

        // Reference a given text file as a collection of Strings.
        // The input can be any hadoop InputFormat
        PCollection<String> tags = pipeline.readTextFile(inputPathTags);
        PCollection<String> movies = pipeline.readTextFile(inputPathMovies);
        PCollection<String> ratings = pipeline.readTextFile(inputPathRates);


        // Get PTable objects for each file
        // Parsing each line according to the separator "::"
        // Also choose which column will be the key column and which will be the value column
        PTable<String, String> movieForTagsPrep = FilePrep.getFileAsPTable(movies, 0, 1);
        PTable<String, String> tagsPrep = FilePrep.getFileAsPTable(tags, 1, 2);

        // Normalize data
        // 1. We may also want to consider using only lower case strings
        //    in case the data is not consistent.
        // 2. Remove null results after any left joins (see next code block)

        // 1.
        // Get most common tag for a movie title
        //

        // Join movies (movie_id, movie_title) and tags (movie_id, tag) tables on movie id
        // Also filter out any null tags - movies that didn't have tags
        // We will end up with (movie_id, (movie_title, movie_tag))
        PTable<String, Pair<String, String>> joinedMovieTags = Join
                .leftJoin(movieForTagsPrep, tagsPrep)
                .filter(new FilterFn<Pair<String, Pair<String, String>>>() {

                    @Override
                    public boolean accept(final Pair<String, Pair<String, String>> pairMoviePair) {
                        if (pairMoviePair.second().second() == null) {
                            return false;
                        }
                        else {
                            return true;
                        }
                    }
                });

        // Key on (movie_title, tag)
        // Set a value of 1 for each pair
        // groupByKey, aggregate.SUM_INTS
        PTable<Pair<String, String>, Long> sumOfTagsPerMovie = PTables
                .swapKeyValue(joinedMovieTags)
                .mapValues(new MapFn<String, Long>() {
                        public Long map(String currValue) { return 1L; }
                    }, (Writables.longs()))
                .groupByKey()
                .combineValues(Aggregators.SUM_LONGS()); // ((movie_title, movie_tag), count)

        // Key on movie_title, groupByKey
        // Aggregation returns the (tag, count) with the highest count
        PTable<String, Pair<String, Long>> maxTagPerTitle = ReorderKV.getReorderedTable(sumOfTagsPerMovie) // returns PTable(String, (String, Integer))
                .groupByKey() // returns PTableGrouped(String, (String, Integer))
                .combineValues(new CombineFn<String, Pair<String, Long>>() {
                    @Override
                    public void process(Pair<String, Iterable<Pair<String, Long>>> input,
                                        Emitter<Pair<String, Pair<String, Long>>> emitter) {
                        String maxTag = null;
                        Long maxValue = 0L;
                        for (Pair<String, Long> dw : input.second()) {
                            if (dw.second() > maxValue) { maxValue = dw.second(); maxTag = dw.first(); }
                        }
                        emitter.emit(Pair.of(input.first(), Pair.of(maxTag, maxValue)));
                    }
                });

        // Leave only the tag
        PTable<String, String> onlyTagPerTitle = maxTagPerTitle
                .mapValues(new MapFn<Pair<String, Long>, String>() {
                    public String map(Pair<String, Long> maxTagPair) { return maxTagPair.first();}
                }, (Writables.strings()));

        // Write result to file
        onlyTagPerTitle.write(At.textFile(outputPathTagsMovies), Target.WriteMode.OVERWRITE);
//        pipeline.writeTextFile(onlyTagPerTitle, outputPathTagsMovies);

        // 2.
        // Get most common genre for a rater
        //

        // Parse collections to PTables.
        // Results:
        // Movies - (movie_id, movie_genre)
        // Ratings - (movie_id, (user_id, rating))
        PTable<String, String> movieForGenresPrep = FilePrep.getMovieFileAsPTable(movies, 0, 2);
        PTable<String, Pair<String, String>> ratingsPrep = FilePrep
                .getRatingsFileAsPTable(ratings, 1, 0, 2);

        // Remove users with null ratings (not sure there are any, but just to be sure)
        // After we're sure users have ratings, throw out the ratings
        PTable<String, String> usersWithRatings = ratingsPrep
                .filter(new FilterFn<Pair<String, Pair<String, String>>>() {
                    @Override
                    public boolean accept(Pair<String, Pair<String, String>> moviesPerUserRating) {
                        if (moviesPerUserRating.second().second() == null) {
                            return false;
                        }
                        else {
                            return true;
                        }
                    }
                })
                .mapValues(new MapFn<Pair<String, String>, String>() {
                    @Override
                    public String map(Pair<String, String> usersAndRatings) {
                        return usersAndRatings.first();
                    }
                }, Writables.strings());

        // Left joining (movie_id, genre) with (movie_id, user_id)
        // Filter movies with no users
        PTable<String, Pair<String, String>> joinedUsersGenres = Join
                .leftJoin(usersWithRatings, movieForGenresPrep)
                .filter(new FilterFn<Pair<String, Pair<String, String>>>() {
                    @Override
                    public boolean accept(Pair<String, Pair<String, String>> moviesPerUserRating) {
                        if (moviesPerUserRating.second().second() == null) {
                            return false;
                        }
                        else {
                            return true;
                        }
                    }
                });

        // Now we don't need the movie_id anymore. Get to (user_id, genre)
        PTable<String, String> usersToGenres = ReorderKV.makeValueIntoKey(joinedUsersGenres);

        // count the appearance of each pair
        PTable<Pair<String, String>, Long> usersToGenresCounted = Aggregate.count(usersToGenres);

        PTable<String, Pair<String, Long>> maxGenrePerUser = ReorderKV.getReorderedTable(usersToGenresCounted) // returns PTable(String, (String, Long))
                .groupByKey() // returns PTableGrouped(String, (String, Long))
                .combineValues(new CombineFn<String, Pair<String, Long>>() {
                    @Override
                    public void process(Pair<String, Iterable<Pair<String, Long>>> input,
                                        Emitter<Pair<String, Pair<String, Long>>> emitter) {
                        String maxTag = null;
                        Long maxValue = 0L;
                        for (Pair<String, Long> dw : input.second()) {
                            if (dw.second() > maxValue) { maxValue = dw.second(); maxTag = dw.first(); }
                        }
                        emitter.emit(Pair.of(input.first(), Pair.of(maxTag, maxValue)));
                    }
                });

        // Leave only the tag
        PTable<String, String> onlyGenrePerUser = maxGenrePerUser
                .mapValues(new MapFn<Pair<String, Long>, String>() {
                    public String map(Pair<String, Long> maxTagPair) { return maxTagPair.first();}
                }, (Writables.strings()));


        // Write result to file
        onlyGenrePerUser.write(At.textFile(outputPathUserGenres), Target.WriteMode.OVERWRITE);

        PipelineResult result = pipeline.done();

        System.out.print("Result " + result.succeeded() + "\n");
        return result.succeeded() ? 0 : 1;
    }
}