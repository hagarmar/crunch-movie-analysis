package com.example.utilities;

import org.apache.crunch.Emitter;
import org.apache.crunch.Pair;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

/**
 * Created by hagar on 12/26/16.
 */
@RunWith(MockitoJUnitRunner.class)
public class GenreSplitterTest {

    @Mock
    private Emitter<Pair<String, String>> emitter;

    private Pair<String, String> testSplittableString = Pair.of("Toy Story (1995)", "Adventure|Animation");
    private Pair<String, String> testUnsplittableString = Pair.of("Toy Story (1995)", "Adventure");

    private Pair<String, String> testSplittableStringExpectedFirst = Pair.of("Toy Story (1995)", "Adventure");
    private Pair<String, String> testSplittableStringExpectedSecond = Pair.of("Toy Story (1995)", "Animation");


    @Test
    public void testSplittableProcess() {
        GenreSplitter splitter = new GenreSplitter();
        splitter.process(testSplittableString, emitter);

        verify(emitter).emit(testSplittableStringExpectedFirst);
        verify(emitter).emit(testSplittableStringExpectedSecond);
        verifyNoMoreInteractions(emitter);
    }


    @Test
    public void testUnsplittableProcess() {
        GenreSplitter splitter = new GenreSplitter();
        splitter.process(testUnsplittableString, emitter);

        verify(emitter).emit(testSplittableStringExpectedFirst);
        verifyNoMoreInteractions(emitter);
    }

}
