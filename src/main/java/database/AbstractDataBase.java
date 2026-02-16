package database;

import java.util.HashSet;
import java.util.Set;

/**
 * Abstract database class holding common data structures
 */
public abstract class AbstractDataBase {
    protected Set<String> categories;
    protected Set<String> languages;
    protected Set<String> linkingWords;

    public AbstractDataBase() {
        categories = new HashSet<>();
        languages = new HashSet<>();
        linkingWords = new HashSet<>();
    }

    /**
     * Record holding a data string and the number of times it appears
     * @param data data itself as string
     * @param times number of times it appears
     */
    public record PairData(String data, int times) implements Comparable<PairData> {
        /**
         * Compares two PairData objects first by times (descending), then by data (ascending)
         * @param o the object to be compared.
         * @return a negative integer, zero, or a positive integer as this object is less than, equal to, or greater than the specified object
         */
        @Override
        public int compareTo(PairData o) {
            int r = o.times - times;
            return r == 0 ?  data.compareTo(o.data) : r;
        }

        /**
         * String representation of PairData
         * @return string in the format "data times"
         */
        @Override
        public String toString() {
            return data + " " + times;
        }
    }

    // Getters and setters
    public Set<String> getCategories() {
        return categories;
    }

    public void setCategories(Set<String> categories) {
        this.categories = categories;
    }

    public Set<String> getLanguages() {
        return languages;
    }

    public void setLanguages(Set<String> languages) {
        this.languages = languages;
    }

    public Set<String> getLinkingWords() {
        return linkingWords;
    }

    public void setLinkingWords(Set<String> linkingWords) {
        this.linkingWords = linkingWords;
    }
}
