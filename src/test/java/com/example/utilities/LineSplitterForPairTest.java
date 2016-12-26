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
public class LineSplitterForPairTest {

    @Mock
    private Emitter<Pair<String, Pair<String, String>>> emitter;

    private String testSplittableString = "a::b::c::d";

    private Pair<String, Pair<String, String>> testSplittableStringExpected = Pair.of("a", Pair.of("b", "c"));


    @Test
    public void testSplittableProcess() {
        LineSplitterForPair splitter = new LineSplitterForPair(
                0,1, 2);
        splitter.process(testSplittableString, emitter);

        verify(emitter).emit(testSplittableStringExpected);
        verifyNoMoreInteractions(emitter);
    }

}
