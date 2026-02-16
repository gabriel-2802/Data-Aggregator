package auxs;

import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Utility class for constants
 */
public final class Constants {
    public static final ObjectMapper MAPPER = new ObjectMapper();
    public static final int MASTER_THREAD = 0;
    public static final String ALL_FILE = "all_articles.txt";
    public static final String WORDS_FILE = "keywords_count.txt";
    public static final String REPORT_FILE = "reports.txt";
    public static final String FILE_EXTENSION = ".txt";
    public static final String LANGUAGE = "english";

    private Constants() {
        // utility class
    }

}
