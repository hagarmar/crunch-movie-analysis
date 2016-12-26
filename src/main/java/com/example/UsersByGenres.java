package com.example;

import com.example.utilities.*;
import org.apache.crunch.*;
import org.apache.crunch.io.At;
import org.apache.crunch.lib.Join;
import org.apache.crunch.types.writable.Writables;

/**
 * Class for getting the most common genre for a rater.
 *
 * Created by hagar on 12/25/16.
 */
public class UsersByGenres {

    static public PTable<String, String> prepMovies(PCollection<String> movies) {
        PTable<String, String> movieTable = movies
                .parallelDo(new LineSplitter(0, 2),
                Writables.tableOf(Writables.strings(), Writables.strings())
                );

        return movieTable.parallelDo(
                new GenreSplitter(),
                Writables.tableOf(Writables.strings(), Writables.strings())
        );
    }


    static public PTable<String, String> prepRatings(PCollection<String> ratings) {
        // Remove users with null ratings (not sure there are any, but just to be sure)
        // After we're sure users have ratings, throw out the ratings
        return ratings
                .parallelDo(new LineSplitterForPair(0, 1, 2),
                    Writables.tableOf(Writables.strings(),
                            Writables.pairs(Writables.strings(), Writables.strings())))
                .filter(new FilterMissingRatings())
                .mapValues(new MapFn<Pair<String, String>, String>() {
                    @Override
                    public String map(Pair<String, String> usersAndRatings) {
                        return usersAndRatings.first();
                    }
                }, Writables.strings());
    }


    static public PTable<String, Pair<String, String>> joinUsersAndGenres(PTable<String, String> users,
                                                                           PTable<String, String> genres) {
        return Join.innerJoin(users, genres);
    }


    static public PTable<String, String> getMaxGenrePerUser(PCollection<String> ratings,
                                                            PCollection<String> movies) {

        PTable<String, String> moviesClean = prepMovies(movies);
        PTable<String, String> usersClean = prepRatings(ratings);

        // Left joining (movie_id, genre) with (movie_id, user_id)
        // Filter movies with no users
        PTable<String, Pair<String, String>> joinedUsersGenres = joinUsersAndGenres(usersClean, moviesClean);

        return ComputeXPerY.countAndMaxXPerY(joinedUsersGenres);
    }


    static public void run(PCollection<String> ratings,
                           PCollection<String> movies,
                           String outputPath) {

        // Leave only the tag
        PTable<String, String> onlyGenrePerUser = getMaxGenrePerUser(ratings, movies);

        // Write result to file
        onlyGenrePerUser.write(At.textFile(outputPath), Target.WriteMode.OVERWRITE);
    }
}
