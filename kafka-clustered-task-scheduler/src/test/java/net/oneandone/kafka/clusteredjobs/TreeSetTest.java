package net.oneandone.kafka.clusteredjobs;

import java.util.Comparator;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author aschoerk
 */
public class TreeSetTest {

    static class Element {

        static AtomicLong identifierFactory = new AtomicLong(1L);

        String content;
        Long identifier = identifierFactory.getAndIncrement();

        public Element(final String content) {
            this.content = content;
        }

        @Override
        public String toString() {
            return content + " " + identifier;
        }
    }
    public static void main(String[] args) {
        // create a new TreeSet with a custom Comparator
        TreeSet<Element> treeSet = new TreeSet<>(new Comparator<Element>() {
            @Override
            public int compare(final Element o1, final Element o2) {
                int result = -o1.content.compareTo(o2.content);
                return result != 0 ? result : o1.identifier.compareTo(o2.identifier);
            }
        });

        // add elements to the TreeSet
        treeSet.add(new Element("apple"));
        treeSet.add(new Element("apple"));
        treeSet.add(new Element("banana"));
        treeSet.add(new Element("orange"));
        treeSet.add(new Element("grape"));

        // print the elements in the TreeSet (in reverse order)
        for (Element s : treeSet) {
            System.out.println(s);
        }
    }
}
