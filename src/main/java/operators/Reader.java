package operators;

import articles.NewsArticle;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import database.ConcurrentDb;
import auxs.Constants;
import database.SequentialDb;

import java.io.File;
import java.io.IOException;
import java.util.List;

/**
 * Operator responsible for reading news articles from JSON files
 */
public class Reader implements Operator {
    private final List<String> filesToRead;
    private final ObjectMapper mapper;
    private final SequentialDb localDb;

    /**
     * Constructs a Reader with a list of files to process.
     *
     * @param filesToRead list of JSON file paths to read articles from
     */
    public Reader(List<String> filesToRead, SequentialDb localDb) {
        this.filesToRead = filesToRead;
        this.mapper = Constants.MAPPER;
        this.localDb = localDb;
    }

    /**
     * Executes the read operation, parsing JSON files and adding articles to the database (ConcurrentSkipListSet)
     */
    @Override
    public void execute() {
        ConcurrentDb db = ConcurrentDb.getInstance();
        int total = 0;

        for (String fileName : filesToRead) {
            File file = new File(fileName);

            try {
                List<NewsArticle> articles = mapper.readValue(
                        file,
                        new TypeReference<List<NewsArticle>>() {}
                );

                for (NewsArticle a : articles) {
                    localDb.addArticle(a);
                    total++;
                }
            } catch (IOException e) {
                System.err.println("Error reading file " + fileName);
                System.err.println(e.getMessage());
            }
        }

        // increments the total number of articles read
        db.incrementArts(total);
    }
}
