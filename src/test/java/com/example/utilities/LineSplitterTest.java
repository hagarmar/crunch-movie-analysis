package com.example.utilities;

import org.apache.crunch.Emitter;
import org.apache.crunch.Pair;
import org.junit.Assert;
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
public class LineSplitterTest {

    @Mock
    private Emitter<Pair<String, String>> emitter;

    private String testSplittableString = "a::b::c::d";
    private String testUnsplittableString = "a<b<c<d";

    private String[] testSplittableStringExpected = new String[] {"a", "b", "c", "d"};
    private String[] testUnsplittableStringExpected = new String[] {"a<b<c<d"};


    private Pair<String, String> testProcessExpected = Pair.of("a", "b");

    @Test
    public void testSplitStringBySeparatorSplittable() {
        String[] testSplittableStringObserved = LineSplitter.splitStringBySeparator(
                testSplittableString, "::");
        Assert.assertArrayEquals(testSplittableStringExpected, testSplittableStringObserved);
    }


    @Test
    public void testSplitStringBySeparatorUnsplittable() {
        String[] testUnsplittableStringObserved = LineSplitter.splitStringBySeparator(
                testUnsplittableString, "::");
        Assert.assertArrayEquals(testUnsplittableStringExpected, testUnsplittableStringObserved);
    }


    @Test
    public void testSplitStringByUnsplittableSeparator() {
        String[] testUnsplittableStringObserved = LineSplitter.splitStringBySeparator(
                testUnsplittableString, "<");
        Assert.assertArrayEquals(testSplittableStringExpected, testUnsplittableStringObserved);
    }

    @Test
    public void testProcess() {
        LineSplitter splitter = new LineSplitter(0, 1);
        splitter.process(testSplittableString, emitter);

        verify(emitter).emit(testProcessExpected);
        verifyNoMoreInteractions(emitter);
    }

    // If we were handling bad inputs, such as
    // bad k, v values, we would need testing for those cases

}
