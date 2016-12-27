package com.example;

import com.sun.tools.javac.util.List;
import org.apache.crunch.PCollection;
import org.apache.crunch.PTable;
import org.apache.crunch.Pair;
import org.apache.crunch.impl.mem.MemPipeline;
import org.apache.crunch.lib.Sort;
import org.apache.crunch.types.writable.Writables;
import org.junit.Assert;
import org.junit.Test;

/**
 * Tests for UserByGenres
 * Created by hagar on 12/25/16.
 */
public class UsersByGenresTest {
    private PCollection<String> movies = MemPipeline.typedCollectionOf(
            Writables.strings(),
            List.of(
                    "1::Toy Story (1995)::Adventure|Animation",
                    "2::Jumanji (1995)::Adventure|Fantasy",
                    "3::Grumpier Old Men (1995)::Children|Fantasy",
                    "4::Waiting to Exhale (1995)::Comedy",
                    "5::Father of the Bride Part II (1995)::Comedy",
                    "0::Test (1970)::Fantasy"
            )
    );


    private PTable<String, String> moviesCleanExpected = MemPipeline.typedTableOf(
            Writables.tableOf(Writables.strings(), Writables.strings()),
            List.of(
                    Pair.of("1", "Adventure"),
                    Pair.of("1", "Animation"),
                    Pair.of("2", "Adventure"),
                    Pair.of("2", "Fantasy"),
                    Pair.of("3", "Children"),
                    Pair.of("3", "Fantasy"),
                    Pair.of("4", "Comedy"),
                    Pair.of("5", "Comedy"),
                    Pair.of("0", "Fantasy")
            )
    );


    private PCollection<String> ratings = MemPipeline.typedCollectionOf(
            Writables.strings(),
            List.of(
                    // user_id, movie_id, rating, timestamp
                    "122::1::5::838985046",
                    "329::1::5::838983392",
                    "316::1::5::838983392",
                    "122::2::5::838985046",
                    "339::2::5::838983392",
                    "316::3::5::838983392",
                    "339::3::5::838983392",
                    "340::3::::838983392",
                    "111::4::3::838983392"
            )
    );


    private PTable<String, String> ratingsCleanExpected = MemPipeline.typedTableOf(
            Writables.tableOf(Writables.strings(), Writables.strings()),
            List.of(
                    // movie_id, user_id
                    Pair.of("1", "122"),
                    Pair.of("1", "329"),
                    Pair.of("1", "316"),
                    Pair.of("2", "122"),
                    Pair.of("2", "339"),
                    Pair.of("3", "316"),
                    Pair.of("3", "339"),
                    Pair.of("4", "111")
            )
    );


    private PTable<String, String> usersByGenresExpected = MemPipeline.typedTableOf(
            Writables.tableOf(Writables.strings(), Writables.strings()),
            List.of(
                    Pair.of("122", "Adventure"),
                    Pair.of("329", "Adventure|Animation"),
                    Pair.of("316", "Adventure|Animation|Children|Fantasy"),
                    Pair.of("339", "Fantasy"),
                    Pair.of("111", "Comedy")
            )
    );


    private PTable<String, Pair<String, String>> joinedUsersGenresExpected =  MemPipeline.typedTableOf(
            Writables.tableOf(Writables.strings(), Writables.pairs(Writables.strings(), Writables.strings())),
            List.of(
                    Pair.of("1", Pair.of("122", "Adventure")),
                    Pair.of("1", Pair.of("329", "Adventure")),
                    Pair.of("1", Pair.of("316", "Adventure")),
                    Pair.of("1", Pair.of("122", "Animation")),
                    Pair.of("1", Pair.of("329", "Animation")),
                    Pair.of("1", Pair.of("316", "Animation")),
                    Pair.of("2", Pair.of("122", "Adventure")),
                    Pair.of("2", Pair.of("339", "Adventure")),
                    Pair.of("2", Pair.of("122", "Fantasy")),
                    Pair.of("2", Pair.of("339", "Fantasy")),
                    Pair.of("3", Pair.of("316", "Children")),
                    Pair.of("3", Pair.of("339", "Children")),
                    Pair.of("3", Pair.of("316", "Fantasy")),
                    Pair.of("3", Pair.of("339", "Fantasy")),
                    Pair.of("4", Pair.of("111", "Comedy"))
            )
    );

    @Test
    public void usersByGenresTest() {
        PTable<String, String> usersByGenresObserved = UsersByGenres.getMaxGenrePerUser(ratings, movies);
        Assert.assertEquals(Sort.sort(usersByGenresObserved).toString(),
                            Sort.sort(usersByGenresExpected).toString());
    }


    private PTable<String, String> getPreppedMovie(PCollection<String> movies) {
        return UsersByGenres.prepMovies(movies);
    }


    private PTable<String, String> getPreppedRatings(PCollection<String> ratings) {
        return UsersByGenres.prepRatings(ratings);
    }


    @Test
    public void prepMoviesTest() {
        PTable<String, String> moviesCleanObserved = getPreppedMovie(movies);
        Assert.assertEquals(moviesCleanExpected.toString(),
                            moviesCleanObserved.toString());

    }

    @Test
    public void prepRatingsTest() {
        PTable<String, String> ratingsCleanObserved = getPreppedRatings(ratings);
        Assert.assertEquals(ratingsCleanExpected.toString(),
                            ratingsCleanObserved.toString());

    }


    @Test
    public void joinMoviesAndTagsTest() {
        PTable<String, String> moviesCleanObserved = getPreppedMovie(movies);
        PTable<String, String> ratingsCleanObserved = getPreppedRatings(ratings);
        PTable<String, Pair<String, String>> joinedUsersAndGenresObserved = UsersByGenres
                .joinUsersAndGenres(ratingsCleanObserved, moviesCleanObserved);

        Assert.assertEquals(joinedUsersGenresExpected.toString(),
                            joinedUsersAndGenresObserved.toString());
    }
}
