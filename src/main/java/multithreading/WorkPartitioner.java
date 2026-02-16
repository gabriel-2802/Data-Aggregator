package multithreading;

import articles.NewsArticle;
import database.ConcurrentDb;
import database.SequentialDb;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * Utility class for partitioning work among multiple threads
 */
public class WorkPartitioner {
    private final int threadId;
    private final int numThreads;

    public WorkPartitioner(int threadId, int numThreads) {
        this.threadId = threadId;
        this.numThreads = numThreads;
    }

    /**
     * Partitions a list of items for the current thread
     *
     * @param items the list of items to partition
     * @param <T>   the type of items in the list
     * @return a sublist containing the items assigned to this thread
     */
    public <T> List<T> partitionList(List<T> items) {
        int numItems = items.size();
        int base = numItems / numThreads;
        int remainder = numItems % numThreads;

        int start = threadId * base + Math.min(threadId, remainder);
        int end = start + base + (threadId < remainder ? 1 : 0);

        return items.subList(start, end);
    }

    /**
     * Partitions a set of items for the current thread
     *
     * @param items the set of items to partition
     * @param <T>   the type of items in the set
     * @return a sublist containing the items assigned to this thread
     */
    public <T> List<T> partitionList(Set<T> items) {
        List<T> itemList = new ArrayList<>(items);
        return partitionList(itemList);
    }
}

