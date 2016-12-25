package com.example;

import org.apache.crunch.*;
import org.apache.crunch.io.At;
import org.apache.crunch.types.writable.Writables;

/**
 * Created by hagar on 12/25/16.
 */
public class UsersByGenres {

    static private PTable<String, String> prepMovies(PCollection<String> movies) {
        return FilePrep.getMovieFileAsPTable(movies, 0, 2);
    }


    static private PTable<String, String> prepRatings(PCollection<String> ratings) {
        // Remove users with null ratings (not sure there are any, but just to be sure)
        // After we're sure users have ratings, throw out the ratings
        return FilePrep
                .getRatingsFileAsPTable(ratings, 1, 0, 2)
                .filter(new FilterFn<Pair<String, Pair<String, String>>>() {
                    @Override
                    public boolean accept(Pair<String, Pair<String, String>> moviesPerUserRating) {
                        if (moviesPerUserRating.second().second() == null) {
                            return false;
                        } else {
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
    }


    static private PTable<String, Pair<String, String>> joinUsersAndGenres(PTable<String, String> users,
                                                                           PTable<String, String> genres) {
        return JoinAndFilter.compute(users, genres);
    }


    static public void run(PCollection<String> ratings,
                           PCollection<String> movies,
                           String outputPath) {

        PTable<String, String> moviesClean = prepMovies(movies);
        PTable<String, String> usersClean = prepRatings(ratings);

        // Left joining (movie_id, genre) with (movie_id, user_id)
        // Filter movies with no users
        PTable<String, Pair<String, String>> joinedUsersGenres = joinUsersAndGenres(usersClean, moviesClean);

        // Leave only the tag
        PTable<String, String> onlyGenrePerUser = ComputeXPerY.countAndMaxXPerY(joinedUsersGenres);

        // Write result to file
        onlyGenrePerUser.write(At.textFile(outputPath), Target.WriteMode.OVERWRITE);
    }
}
