package com.example;

import org.apache.crunch.*;
import org.apache.crunch.fn.Aggregators;
import org.apache.crunch.impl.mr.MRPipeline;
//import org.apache.crunch.impl.mem.MemPipeline;
import org.apache.crunch.io.At;
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

        if (args.length < 3) {
            System.err.println("Usage: hadoop jar crunchtask-1.0-SNAPSHOT-job.jar"
                    + " input");
            System.err.println();
            GenericOptionsParser.printGenericCommandUsage(System.err);
            return 1;
        }

        // String inputPathRates = args[0];
        String inputPathTags = args[0];
        String inputPathMovies = args[1];
        String outputPathTagsMovies = args[2];

        // Create an object to coordinate pipeline creation and execution.
        // Creating the pipeline instance
        // We will usually use MRPipeline. there's also MEMPipeline and SparkPipeline
        // Here it will get the name of the current class and the command line args
        Pipeline pipeline = new MRPipeline(MovieAnalysis.class, getConf());

        // Reference a given text file as a collection of Strings.
        // this is kind of like parallelizing in spark - specifying input for job
        // the input can be any hadoop InputFormat
        // Here we parallelize the a text file
        //PCollection<String> ratings = pipeline.readTextFile(inputPathRates);
        PCollection<String> tags = pipeline.readTextFile(inputPathTags);
        PCollection<String> movies = pipeline.readTextFile(inputPathMovies);

        // Get most common tag for a movie title

        // Get PTable objects for each file
        PTable<String, String> movieForTagsPrep = FilePrep.getFileAsPTable(movies, 0, 1);
        PTable<String, String> tagsPrep = FilePrep.getTagFileAsPTable(tags, 1, 2);

        // Join movies and tags tables on movie id
        // get (movie_id, movie_title) and (movie_id, tag)
        // join on movie_id
        PTable<String, Pair<String, String>> joinedMovieTags = Join
                .leftJoin(movieForTagsPrep, tagsPrep); // now we have (movie_id, (movie_title, movie_tag))


        // key on Pair.of(movie_title, tag), groupbykey, aggregate.SUM_INTS
        PTable<Pair<String, String>, Integer> sumOfTagsPerMovie = PTables
                .swapKeyValue(joinedMovieTags)
                .mapValues(new MapFn<String, Integer>() {
                        public Integer map(String currValue) { return 1; }
                    }, (Writables.ints()))
                .groupByKey()
                .combineValues(Aggregators.SUM_INTS()); // ((movie_title, movie_tag), count)

        PTable<String, Pair<String, Integer>> maxTagPerTitle = ReorderKV.getReorderedTable(sumOfTagsPerMovie) // returns PTable(String, (String, Integer))
                .groupByKey() // returns PTableGrouped(String, (String, Integer))
                .combineValues(new CombineFn<String, Pair<String, Integer>>() {
                    @Override
                    public void process(Pair<String, Iterable<Pair<String, Integer>>> input,
                                        Emitter<Pair<String, Pair<String, Integer>>> emitter) {
                        String maxTag = "None";
                        Integer maxValue = 0;
                        for (Pair<String, Integer> dw : input.second()) {
                            if (dw.second() > maxValue) { maxValue = dw.second(); maxTag = dw.first(); }
                        }
                        emitter.emit(Pair.of(input.first(), Pair.of(maxTag, maxValue)));
                    }
                });

        // leave only the tag
        PTable<String, String> onlyTagPerTitle = maxTagPerTitle
                .mapValues(new MapFn<Pair<String, Integer>, String>() {
                    public String map(Pair<String, Integer> maxTagPair) { return maxTagPair.first();}
                }, (Writables.strings()));

//        pipeline.writeTextFile(onlyTagPerTitle, outputPathTagsMovies);
//        sumOfTagsPerMovie.write(At.textFile(outputPathTagsMovies));
//        pipeline.writeTextFile(test, outputPathTagsMovies);
        pipeline.writeTextFile(maxTagPerTitle, outputPathTagsMovies);
        System.out.print("G\n");
//
//
//
//        // Get most common genre for a rater
//        PTable<String, String> movieForGenresPrep = FilePrep.getFileAsPTable(movies, 0, 2);
//        PTable<String, String> ratingsPrep = FilePrep.getFileAsPTable(ratings, 1, 2);
//
//        // Join ratings and movies tables on movie id
//        PTable<String, Pair<String, String>> joinedMovieRatings = Join.leftJoin(movieForGenresPrep, ratingsPrep);
//
        PipelineResult result = pipeline.done();
        System.out.print("Result " + result.succeeded() + "\n");
        return result.succeeded() ? 0 : 1;
    }
}