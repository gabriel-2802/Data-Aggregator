package operators;

import articles.NewsArticle;
import auxs.Constants;
import database.ConcurrentDb;
import multithreading.WorkPartitioner;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 *  Class that writes final and auxiliary files processed by the thread
 */
public class Writer implements Operator {
    // used to partion lists
    private final WorkPartitioner partitioner;
    private final ConcurrentDb db;
    private final String prefixToFile;

    public Writer(WorkPartitioner partitioner, int prefixToFile) {
        this.partitioner = partitioner;
        db = ConcurrentDb.getInstance();
        this.prefixToFile = String.valueOf(prefixToFile) + "_";
    }

    @Override
    public void execute() {
        writeCategories();
        writeLanguages();
        writePartialData();
        writePartialKeyWords();
    }

    /**
     * Partitions the list of categories and writes a file for each category
     */
    private void writeCategories() {
        List<Map.Entry<String, List<String>>> listToPrint = partitioner.partitionList(db.getCategoryToArticle().entrySet());

        for (var  entry : listToPrint) {
            String category = entry.getKey();
            String normalizedCategory = auxs.Utils.normalizeCategory(category);
            String filename = normalizedCategory + Constants.FILE_EXTENSION;

            writeListToFile(filename, entry.getValue().stream().distinct().collect(Collectors.toList()));
        }
    }

    /**
     * Partitions the list of languages and writes a file for each category
     */
    private void writeLanguages() {
        List<Map.Entry<String, List<String>>> listToPrint = partitioner.partitionList(db.getLanguageToArticle().entrySet());
        for  (var  entry : listToPrint) {
            String language = entry.getKey();
            String filename = language + Constants.FILE_EXTENSION;
            writeListToFile(filename, entry.getValue());
        }
    }

    /**
     * Writes this thread's partition of article data to a partial file i_all_articles.txt
     */
    private void writePartialData() {
        List<NewsArticle> articles = partitioner.partitionList(db.getArticles());
        String filename = prefixToFile +  Constants.ALL_FILE;
        writeListToFile(filename, articles.stream().map(a -> a.getUuid() + " " + a.getPublished()).collect(Collectors.toList()));
    }

    /**
     * Writes this thread's partition of keyword occurrence data to a partial file
     */
    private void writePartialKeyWords() {
        var data = partitioner.partitionList(db.getKeywordsData());
        String filename = prefixToFile +  Constants.WORDS_FILE;
        writeListToFile(filename, data.stream().map(ConcurrentDb.PairData::toString).collect(Collectors.toList()));
    }

    /**
     * Writes a list of strings to a file, one item per line
     *
     * @param filename the output file path
     * @param items the list of strings to write
     */
    private void writeListToFile(String filename, List<String> items) {
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(filename))) {
            for (String item : items) {
                writer.write(item);
                writer.newLine();
            }
        } catch (IOException e) {
            System.err.println("Error writing to file " + filename + ": " + e.getMessage());
        }
    }
}
