package operators;

import auxs.Constants;
import auxs.Utils;
import database.ConcurrentDb;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;

/**
 * Operator responsible for writing the final aggregated statistics report (performed by MASTER)
 */
public class ReportWriter implements Operator {
    private final ConcurrentDb db;

    /**
     * Constructs a ReportWriter with access to the concurrent database
     */
    public ReportWriter() {
        this.db = ConcurrentDb.getInstance();
    }

    /**
     * Executes the report writing operation, generating the reports.txt file
     */
    @Override
    public void execute() {
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(Constants.REPORT_FILE))) {
            writer.write("duplicates_found - " + db.getDuplicatesRemoved());
            writer.write("\n");

            writer.write("unique_articles - " + db.getArticles().size());
            writer.write("\n");

            writer.write("best_author - " + db.getAuthorData());
            writer.write("\n");

            writer.write("top_language - " + db.getLanguageData());
            writer.write("\n");

            writer.write("top_category - " + Utils.normalizeCategory(db.getCategoryData().data()) + " " + db.getCategoryData().times());
            writer.write("\n");

            writer.write("most_recent_article - " + db.getMostRecentArticle());
            writer.write("\n");

            writer.write("top_keyword_en - " + db.getKwData());
            writer.write("\n");

        } catch (IOException e) {
            System.err.println(e.getMessage());
        }
    }

}
