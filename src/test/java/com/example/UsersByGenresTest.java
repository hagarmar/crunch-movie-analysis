package com.example;

import com.sun.tools.javac.util.List;
import org.apache.crunch.PCollection;
import org.apache.crunch.PTable;
import org.apache.crunch.Pair;
import org.apache.crunch.impl.mem.MemPipeline;
import org.apache.crunch.types.writable.Writables;
import org.junit.Assert;
import org.junit.Test;

/**
 * Created by hagar on 12/25/16.
 */
public class UsersByGenresTest {
    private PCollection<String> movies = MemPipeline.typedCollectionOf(
            Writables.strings(),
            List.of(
                    "1::Toy Story (1995)::Adventure|Animation|Children|Comedy|Fantasy",
                    "2::Jumanji (1995)::Adventure|Children|Fantasy",
                    "3::Grumpier Old Men (1995)::Adventure|Children|Fantasy",
                    "4::Waiting to Exhale (1995)::Comedy|Drama|Romance",
                    "5::Father of the Bride Part II (1995)::Comedy",
                    "0::Test (1970)::Fantasy"
            )
    );


    private PTable<String, String> moviesCleanExpected = MemPipeline.typedTableOf(
            Writables.tableOf(Writables.strings(), Writables.strings()),
            List.of(
                    Pair.of("1", "Adventure|Animation"),
                    Pair.of("2", "Adventure|Fantasy"),
                    Pair.of("3", "Children|Fantasy"),
                    Pair.of("4", "Comedy"),
                    Pair.of("5", "Comedy"),
                    Pair.of("0", "Fantasy")
            )
    );


    private PCollection<String> ratings = MemPipeline.typedCollectionOf(
            Writables.strings(),
            List.of(
                    "1::122::5::838985046",
                    "1::329::5::838983392",
                    "1::316::5::838983392",
                    "2::122::5::838985046",
                    "2::339::5::838983392",
                    "3::316::5::838983392",
                    "3::339::5::838983392",
                    "3::340::null::838983392",
                    "4::111::3::838983392"
            )
    );


    private PTable<String, String> ratingsCleanExpected = MemPipeline.typedTableOf(
            Writables.tableOf(Writables.strings(), Writables.strings()),
            List.of(
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
                    Pair.of("1", Pair.of("122", "Animation")),
                    Pair.of("1", Pair.of("329", "Adventure")),
                    Pair.of("1", Pair.of("329", "Animation")),
                    Pair.of("1", Pair.of("316", "Adventure")),
                    Pair.of("1", Pair.of("316", "Animation")),
                    Pair.of("2", Pair.of("122", "Adventure")),
                    Pair.of("2", Pair.of("122", "Fantasy")),
                    Pair.of("2", Pair.of("339", "Adventure")),
                    Pair.of("2", Pair.of("339", "Fantasy")),
                    Pair.of("3", Pair.of("316", "Children")),
                    Pair.of("3", Pair.of("316", "Fantasy")),
                    Pair.of("3", Pair.of("339", "Children")),
                    Pair.of("3", Pair.of("339", "Fantasy")),
                    Pair.of("4", Pair.of("111", "Comedy"))
            )
    );

    @Test
    public void usersByGenresTest() {
        PTable<String, String> usersByGenresObserved = UsersByGenres.getMaxGenrePerUser(ratings, movies);
        Assert.assertEquals((usersByGenresObserved).toString(),
                (usersByGenresExpected).toString());
    }


    private PTable<String, String> getPreppedMovie(PCollection<String> movies) {
        return UsersByGenres.prepMovies(movies);
    }


    private PTable<String, String> getPreppedRatings(PCollection<String> ratings) {
        return UsersByGenres.prepRatings(movies);
    }


    @Test
    public void prepMoviesTest() {
        PTable<String, String> moviesCleanObserved = getPreppedMovie(movies);
        Assert.assertEquals(moviesCleanExpected.toString(), moviesCleanObserved.toString());

    }

    @Test
    public void prepRatingsTest() {
        PTable<String, String> ratingsCleanObserved = getPreppedRatings(ratings);
        Assert.assertEquals(ratingsCleanExpected.toString(), ratingsCleanObserved.toString());

    }


    @Test
    public void joinMoviesAndTagsTest() {
        PTable<String, String> moviesCleanObserved = getPreppedMovie(movies);
        PTable<String, String> ratingsCleanObserverd = getPreppedRatings(ratings);
        PTable<String, Pair<String, String>> joinedUsersAndGenresObserved = UsersByGenres
                .joinUsersAndGenres(ratingsCleanObserverd, moviesCleanObserved);

        Assert.assertEquals(joinedUsersGenresExpected.toString(), joinedUsersAndGenresObserved.toString());
    }
}
