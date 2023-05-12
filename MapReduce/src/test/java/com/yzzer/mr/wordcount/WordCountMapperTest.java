package com.yzzer.mr.wordcount;

import edu.stanford.nlp.simple.Document;
import edu.stanford.nlp.simple.Sentence;
import edu.stanford.nlp.simple.Token;
import junit.framework.TestCase;
import org.junit.Test;

import java.util.Arrays;

public class WordCountMapperTest {

    @Test
    public void nlpTest() {

        Document doc = new Document("yes zeal acting add your text here! It can contained multiple sentences.");
        for (Sentence sent : doc.sentences()) {  // Will iterate over two sentences
            // We're only asking for words -- no need to load any models yet
//            System.out.println("The second word of the sentence '" + sent + "' is " + sent.word(1));
//            System.out.println("The tag of the second word of the sentence '" + sent + "' is " + sent.posTag(2));
//            // When we ask for the lemma, it will load and run the part of speech tagger
//            System.out.println("The third lemma of the sentence '" + sent + "' is " + sent.lemma(2));
//            // When we ask for the parse, it will load and run the parser
//            System.out.println("The parse of the sentence '" + sent + "' is " + sent.parse());
            // ...
            for (Token token : sent.tokens()) {
                System.out.println(token.lemma());
                System.out.println(token.posTag());
            }
        }

    }

    @Test
    public void regexTest() {
        String words = " a";
        String[] split = words.split("[^a-zA-Z]+");
        System.out.println(Arrays.toString(split));
    }


}