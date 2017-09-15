package test;

import java.util.Arrays;
import java.util.Iterator;

public class WordCountTest {
    public static void main(String[] args) {
        String[] strings = new String[]{"a","b","c","d"};
        Iterator<String> iterator = Arrays.asList(strings).iterator();
        System.out.println(iterator.next());
    }
}
