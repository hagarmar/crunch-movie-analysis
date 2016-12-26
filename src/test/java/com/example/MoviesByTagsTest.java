package com.example;

import com.sun.tools.javac.util.List;
import org.apache.crunch.PCollection;
import org.apache.crunch.PTable;
import org.apache.crunch.Pair;
import org.apache.crunch.impl.mem.MemPipeline;
import org.apache.crunch.types.writable.Writables;
import org.junit.Test;
import org.junit.Assert;

/**
 * Tests for MoviesByTags.
 * Created by hagar on 12/25/16.
 */
public class MoviesByTagsTest {
    private PCollection<String> movies = MemPipeline.typedCollectionOf(
            Writables.strings(),
            List.of(
                    "1::Toy Story (1995)::Adventure|Animation|Children|Comedy|Fantasy",
                    "2::Jumanji (1995)::Adventure|Children|Fantasy",
                    "3::Grumpier Old Men (1995)::Comedy|Romance",
                    "4::Waiting to Exhale (1995)::Comedy|Drama|Romance",
                    "5::Father of the Bride Part II (1995)::Comedy",
                    "0::Test (1970)::Fantasy"
            )
    );


    private PTable<String, String> moviesCleanExpected = MemPipeline.typedTableOf(
            Writables.tableOf(Writables.strings(), Writables.strings()),
            List.of(
                    Pair.of("1", "Toy Story (1995)"),
                    Pair.of("2", "Jumanji (1995)"),
                    Pair.of("3", "Grumpier Old Men (1995)"),
                    Pair.of("4", "Waiting to Exhale (1995)"),
                    Pair.of("5", "Father of the Bride Part II (1995)"),
                    Pair.of("0", "Test (1970)")
            )
    );


    private PCollection<String> tags = MemPipeline.typedCollectionOf(
            Writables.strings(),
            List.of(
                    "100::1::toys::1188263867",
                    "101::1::toys::1188263867",
                    "102::1::children::1188263835",
                    "101::1::hanks::1188263835",
                    "200::2::children::1188263835",
                    "201::2::game::1188263756",
                    "300::3::old men::1188263880",
                    "301::3::old men::1188263880",
                    "301::11::meh::1188263880"
                    )
    );


    private PTable<String, String> tagsCleanExpected = MemPipeline.typedTableOf(
            Writables.tableOf(Writables.strings(), Writables.strings()),
            List.of(
                    Pair.of("1", "toys"),
                    Pair.of("1", "toys"),
                    Pair.of("1", "children"),
                    Pair.of("1", "hanks"),
                    Pair.of("2", "children"),
                    Pair.of("2", "game"),
                    Pair.of("3", "old men"),
                    Pair.of("3", "old men"),
                    Pair.of("11", "meh")
            )
    );


    private PTable<String, String> moviesByTagsExpected = MemPipeline.typedTableOf(
            Writables.tableOf(Writables.strings(), Writables.strings()),
            List.of(
                    Pair.of("Grumpier Old Men (1995)", "old men"),
                    Pair.of("Jumanji (1995)", "children|game"),
                    Pair.of("Toy Story (1995)", "toys")
            )
    );


    private PTable<String, Pair<String, String>> joinedMoviesTagsExpected =  MemPipeline.typedTableOf(
            Writables.tableOf(Writables.strings(), Writables.pairs(Writables.strings(), Writables.strings())),
            List.of(
                    Pair.of("1", Pair.of("Toy Story (1995)", "toys")),
                    Pair.of("1", Pair.of("Toy Story (1995)", "toys")),
                    Pair.of("1", Pair.of("Toy Story (1995)", "children")),
                    Pair.of("1", Pair.of("Toy Story (1995)", "hanks")),
                    Pair.of("2", Pair.of("Jumanji (1995)", "children")),
                    Pair.of("2", Pair.of("Jumanji (1995)", "game")),
                    Pair.of("3", Pair.of("Grumpier Old Men (1995)", "old men")),
                    Pair.of("3", Pair.of("Grumpier Old Men (1995)", "old men"))
            )
    );

    @Test
    public void moviesByTagsTest() {
        PTable<String, String> moviesByTagsObserved = MoviesByTags.getMaxTagPerTitle(movies, tags);
        Assert.assertEquals((moviesByTagsObserved).toString(),
                            (moviesByTagsExpected).toString());
    }


    private PTable<String, String> getPreppedMovie(PCollection<String> movies) {
        return MoviesByTags.prepMovies(movies);
    }


    private PTable<String, String> getPreppedTags(PCollection<String> movies) {
        return MoviesByTags.prepTags(movies);
    }


    @Test
    public void prepMoviesTest() {
        PTable<String, String> moviesCleanObserved = getPreppedMovie(movies);
        Assert.assertEquals(moviesCleanObserved.toString(),
                            moviesCleanExpected.toString());

    }

    @Test
    public void prepTagsTest() {
        PTable<String, String> tagsCleanObserved = getPreppedTags(tags);
        Assert.assertEquals(tagsCleanObserved.toString(),
                            tagsCleanExpected.toString());

    }


    @Test
    public void joinMoviesAndTagsTest() {
        PTable<String, String> moviesCleanObserved = getPreppedMovie(movies);
        PTable<String, String> tagsCleanObserved = getPreppedTags(tags);
        PTable<String, Pair<String, String>> joinMoviesAndTagsObserved = MoviesByTags
                .joinMoviesAndTags(moviesCleanObserved, tagsCleanObserved);

        Assert.assertEquals(joinMoviesAndTagsObserved.toString(),
                            joinedMoviesTagsExpected.toString());
    }
}

