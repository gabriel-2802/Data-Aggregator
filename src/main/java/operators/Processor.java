package operators;

import auxs.Constants;
import auxs.Utils;
import database.ConcurrentDb;
import database.SequentialDb;

public class Processor implements Operator {
    private final SequentialDb db;

    public Processor(SequentialDb db) {
        this.db = db;
    }

    /**
     * Executes the processing operation on all articles in the database partition
     */
    @Override
    public void execute() {
        removeDuplicates();

        db.getArticleSet().forEach(article -> {
            // process language
            String language = article.getLanguage();
            if (db.getLanguages().contains(language)) {
                db.addArticleToLanguage(language, article.getUuid());
            }

            // process categories
            article.getCategories().stream()
                    .filter(db.getCategories()::contains)
                    .forEach(c -> db.addArticleToCategory(c, article.getUuid()));

            // process author occurrences
            db.incrementAuthorOccurrence(article.getAuthor());

            // update most recent article
            if (db.getMostRecentArticle() == null ||
                    article.getPublished().compareTo(db.getMostRecentArticle().getPublished()) > 0) {
                db.setMostRecentArticle(article);
            }

            // process keywords for english arts only
            if (!language.equals(Constants.LANGUAGE)) return;

            var wordsInArticle = Utils.textToWords(article.getText());
            wordsInArticle.forEach(word -> {
                if (db.getLinkingWords().contains(word)) return;

                db.incrementKeywordOccurrence(word);
            });
        });

        sortEntries();
    }

    private void removeDuplicates() {
        ConcurrentDb mainDb = ConcurrentDb.getInstance();
        db.removeDuplicates(mainDb.getUuidCount(), mainDb.getTitleCount());

    }

    /**
     * Sorts category and language article lists.
     */
    private void sortEntries() {
        db.getCategories().forEach(db::sortCategoryArticles);
        db.getLanguages().forEach(db::sortLanguageArticles);
    }
}
