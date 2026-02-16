package auxs;

import java.util.*;

/**
 * Utility class for methods used inside the programe
 */
public final class Utils {
    private Utils() {
        // utility class
    }


    public static String normalizeCategory(String category) {
        if (category == null) return null;
        return  category.replace(",", "").trim().replaceAll("\\s+", "_");
    }

    /**
     * Creates the output file path required
     * @param path initial path
     * @param y final destination
     * @return path
     */
    public static String replaceLastPathElement(String path, String y) {
        if (path == null || path.isEmpty()) {
            return y;
        }

        StringBuilder sb = new StringBuilder(path);

        int lastSlash = sb.lastIndexOf("/");
        if (lastSlash == -1) {
            return y;
        }

        sb.delete(lastSlash + 1, sb.length());
        sb.append(y);
        return sb.toString();
    }

    /**
     * Converts a text into a set of words
     * @param text input text
     * @return set of unique words
     */
    public static Set<String> textToWords(String text) {
        Set<String> wordsSet = new HashSet<>();

        if (text == null) return null;

        // lowercase
        String lower = text.toLowerCase();

        // split into words
        String[] words = lower.split("\\s+");

        for (String w : words) {
            StringBuilder sb = new StringBuilder();

            // keep only letters
            for (int i = 0; i < w.length(); i++) {
                char c = w.charAt(i);
                if (c >= 'a' && c <= 'z') {
                    sb.append(c);
                }

            }

            if (!sb.isEmpty()) {
                wordsSet.add(sb.toString());
            }
        }

        return wordsSet;
    }

    /**
     * Merges multiple sorted lists into a single sorted list
     * @param lists input lists
     * @param <T> type of elements
     * @return merged sorted list
     */
    public static <T extends Comparable<T>> List<T> mergeLists(List<List<T>> lists) {
        if (lists.size() == 1) {
            return new ArrayList<>(lists.getFirst());
        }
        List<T> result = new ArrayList<>();

        PriorityQueue<Pointer<T>> pq = new PriorityQueue<>();

        // initialize heap with the first element of each list
        for (int i = 0; i < lists.size(); i++) {
            List<T> list = lists.get(i);
            if (!list.isEmpty()) {
                pq.add(new Pointer<>(list.getFirst(), i, 0));
            }
        }

        // k-way merge
        while (!pq.isEmpty()) {
            Pointer<T> p = pq.poll();
            result.add(p.value);

            // push next element from the same list
            List<T> currentList = lists.get(p.listId);
            int nextIndex = p.index + 1;

            if (nextIndex < currentList.size()) {
                pq.add(new Pointer<>(
                        currentList.get(nextIndex),
                        p.listId,
                        nextIndex
                ));
            }
        }

        return result;
    }

    /**
     * Pointer class for k-way merge
     * @param <T> type of elements
     */
    private static class Pointer<T extends Comparable<T>> implements Comparable<Pointer<T>> {
        T value;
        int listId;
        int index;

        /**
         * Constructs a Pointer with the specified values.
         *
         * @param value the current value
         * @param listId the ID of the list this pointer references
         * @param index the index within that list
         */
        Pointer(T value, int listId, int index) {
            this.value = value;
            this.listId = listId;
            this.index = index;
        }

        /**
         * Compares pointers by their values.
         *
         * @param o the other pointer to compare to
         * @return comparison result
         */
        @Override
        public int compareTo(Pointer<T> o) {
            return this.value.compareTo(o.value);
        }
    }

}
