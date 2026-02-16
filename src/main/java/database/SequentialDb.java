package database;

import articles.NewsArticle;
import java.util.*;
import java.util.stream.Collectors;

/**
 * A sequential (non-thread-safe) implementation of the database for storing and managing news articles
 */
public class SequentialDb extends AbstractDataBase {
    private final Set<NewsArticle> articleSet;
    private final Map<String, List<String>> categoryToArticle;
    private final Map<String, List<String>> languageToArticle;
    private final Map<String, Integer> keyWordsOccurences;
    private final Map<String, Integer> authorOccurences;
    private NewsArticle mostRecentArticle;

    private final Map<String, Integer> uuidCount;
    private final Map<String, Integer> titleCount;

    /**
     * Constructs a new SequentialDb with the specified articles and metadata
     *
     * @param languages the set of valid language codes
     * @param categories the set of valid category names
     * @param keyWords the set of linking words to track
     */
    public SequentialDb(Set<String> languages, Set<String> categories, Set<String> keyWords) {
        this.articleSet = new HashSet<>();
        this.languages = languages;
        this.categories = categories;
        this.linkingWords = keyWords;

        categoryToArticle = new HashMap<>();
        languageToArticle = new HashMap<>();
        keyWordsOccurences = new HashMap<>();
        authorOccurences = new HashMap<>();
        mostRecentArticle = null;

        // Initialize category and language maps with empty lists
        categories.forEach(category -> {categoryToArticle.put(category, new ArrayList<>());});
        languages.forEach(language -> {languageToArticle.put(language, new ArrayList<>());});

        // Initialize dedup maps
        uuidCount = new HashMap<>();
        titleCount = new HashMap<>();
    }

    public void addArticle(NewsArticle article) {
        articleSet.add(article);
        uuidCount.merge(article.getUuid(), 1, Integer::sum);
        titleCount.merge(article.getTitle(), 1, Integer::sum);
    }

    /**
     * Adds an article to the language index
     *
     * @param language the language code
     * @param uuid the UUID of the article to add
     */
    public void addArticleToLanguage(String language, String uuid) {
        languageToArticle.get(language).add(uuid);
    }

    /**
     * Adds an article to the category index
     *
     * @param category the category name
     * @param uuid the UUID of the article to add
     */
    public void addArticleToCategory(String category, String uuid) {
        categoryToArticle.get(category).add(uuid);
    }

    /**
     * Increments the occurrence count for the specified author
     *
     * @param author the name of the author
     */
    public void incrementAuthorOccurrence(String author) {
        authorOccurences.merge(author, 1, Integer::sum);
    }

    /**
     * Increments the occurrence count for the specified keyword
     *
     * @param keyword the keyword to increment
     */
    public void incrementKeywordOccurrence(String keyword) {
        keyWordsOccurences.merge(keyword, 1, Integer::sum);
    }

    /**
     * Sorts the article UUIDs for the specified category and removes duplicates, sorted by their UUID
     *
     * @param category the category whose articles should be sorted
     */
    public void sortCategoryArticles(String category) {
        categoryToArticle.computeIfPresent(category, (c, list) ->
                list.stream()
                        .distinct()
                        .sorted()
                        .collect(Collectors.toList())
        );
    }

    /**
     * Sorts the article UUIDs for the specified language
     *
     * @param language the language whose articles should be sorted
     */
    public void sortLanguageArticles(String language) {
        Collections.sort(languageToArticle.get(language));
    }

    public void removeDuplicates(Map<String, Integer> uuidMap, Map<String, Integer> titleMap) {
        articleSet.removeIf(article -> uuidMap.get(article.getUuid()) > 1 || titleMap.get(article.getTitle()) > 1);
    }

    public List<NewsArticle> getArticles() {
        var articles = new ArrayList<>(articleSet);
        Collections.sort(articles);
        return articles;
    }

    // GETTERS AND SETTERS

    public  Set<NewsArticle> getArticleSet() {
        return articleSet;
    }

    public Map<String, List<String>> getCategoryToArticle() {
        return categoryToArticle;
    }

    public Map<String, List<String>> getLanguageToArticle() {
        return languageToArticle;
    }

    public Map<String, Integer> getKeyWordsOccurences() {
        return keyWordsOccurences;
    }

    public Map<String, Integer> getAuthorOccurences() {
        return authorOccurences;
    }

    public NewsArticle getMostRecentArticle() {
        return mostRecentArticle;
    }

    public void setMostRecentArticle(NewsArticle mostRecentArticle) {
        this.mostRecentArticle = mostRecentArticle;
    }

    public Map<String, Integer> getUuidCount() {
        return uuidCount;
    }

    public Map<String, Integer> getTitleCount() {
        return titleCount;
    }
}
