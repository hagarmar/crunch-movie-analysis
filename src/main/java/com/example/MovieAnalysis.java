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

        // get most common tag per movie
        MoviesByTags.run(movies, tags, outputPathTagsMovies);

        // get most common genre per rater
//        UsersByGenres.run(ratings, movies, outputPathUserGenres);

        PipelineResult result = pipeline.done();

        System.out.print("Result " + result.succeeded() + "\n");
        return result.succeeded() ? 0 : 1;
    }
}