package database;

import articles.NewsArticle;
import auxs.Utils;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A thread-safe singleton database implementation for storing and managing news articles
 */
public class ConcurrentDb extends AbstractDataBase{
    private static final ConcurrentDb INSTANCE =  new ConcurrentDb();

    // data storage (skiplistset)
    private List<NewsArticle>  articles;

    // data about all articles (no need for sync)
    private final Map<String, List<String>> categoryToArticle = new HashMap<>();
    private final Map<String, List<String>> languageToArticle = new HashMap<>();
    private final Set<PairData> keywordsData = new TreeSet<>();

    // stats data
    private NewsArticle mostRecentArticle;
    private PairData authorData;
    private PairData languageData;
    private PairData categoryData;
    private PairData kwData;
    private AtomicInteger counter = new AtomicInteger(0);

    // dedup
    private final Map<String, Integer> uuidCount = new HashMap<>();
    private final Map<String, Integer> titleCount = new HashMap<>();

    // used to collect data from all threads
    private final List<SequentialDb>  partialDbs;
    private final Map<String, Integer> keyWordsOccurences = new HashMap<>();

    /**
     * Functional interface representing a merge operation that combines data from partial databases
     */
    public interface MergeFunction {
        /**
         * Executes the merge computation.
         */
        void compute();
    }

    /**
     * Private constructor for singleton pattern
     */
    private ConcurrentDb() {
        super();
//        articles = new ConcurrentSkipListSet<>();

        partialDbs = new ArrayList<>();
    }

    /**
     * Returns the singleton instance of ConcurrentDb.
     *
     * @return the singleton ConcurrentDb instance
     */
    public static ConcurrentDb getInstance() {
        return INSTANCE;
    }

    public void initPartialDbs(int numThreads) {
        for (int i = 0; i < numThreads; i++) {
            partialDbs.add(new SequentialDb(languages, categories, linkingWords));
        }
    }

    public SequentialDb getPartialDb(int idx) {
        return partialDbs.get(idx);
    }

    public void generateGlobalDedupMaps() {
        for (var db : partialDbs) {
            db.getUuidCount().forEach((uuid, count) -> {
                uuidCount.merge(uuid, count, Integer::sum);
            });
            db.getTitleCount().forEach((title, count) -> {
                titleCount.merge(title, count, Integer::sum);
            });
        }
    }

    public void generateGlobalArticleList() {
        List<List<NewsArticle>> allLists = new ArrayList<>();
        partialDbs.forEach(db -> allLists.add(db.getArticles()));
        articles = Utils.mergeLists(allLists);
    }

    /**
     * Returns a list of merge operations to be executed for consolidating partial databases
     *
     * @return a list of MergeFunction operations to execute
     */
    public List<MergeFunction> getMergeOperations() {
        return List.of(
                this::mergeCategories,
                this::mergeAuthor,
                this::mergeMostRecentArticle,
                this::mergeLanguages,
                this::mergeKeyWords
        );
    }

    /**
     * Merges category data from all partial databases
     */
    public void mergeCategories() {
        for (String category : categories) {
            List<List<String>> allLists = new ArrayList<>();

            partialDbs.forEach(db ->
                    allLists.add(db.getCategoryToArticle().get(category))
            );

            var mergeList = Utils.mergeLists(allLists);

            if (!mergeList.isEmpty()) {
                categoryToArticle.put(category, mergeList);
            }
        }

        generateTopCategory();
    }

    /**
     * Determines the most popular category based on unique article count
     */
    private void generateTopCategory() {
        String categ = "";
        int times = -1;

        for (var entry : categoryToArticle.entrySet()) {
            int eTimes = entry.getValue().size();
            if (eTimes > times) {
                categ = entry.getKey();
                times = eTimes;
            } else if (eTimes == times && entry.getKey().compareTo(categ) < 0) {
                categ = entry.getKey();
            }
        }

        categoryData = new PairData(categ, times);
    }

    /**
     * Merges language data from all partial databases
     */
    public void mergeLanguages() {
        for (String language : languages) {
            List<List<String>> allLists = new ArrayList<>();
            partialDbs.forEach(db -> allLists.add(db.getLanguageToArticle().get(language)));

            var mergedList = Utils.mergeLists(allLists);
            if (!mergedList.isEmpty()) {
                languageToArticle.put(language, mergedList);
            }
        }

        generateTopLanguage();
    }

    /**
     * Determines the most popular language based on article count
     */
    private void generateTopLanguage() {
        String topLanguage = "";
        int times = -1;

        for (var entry : languageToArticle.entrySet()) {
            int entryTimes = entry.getValue().size();
            if (entryTimes > times) {
                topLanguage = entry.getKey();
                times = entryTimes;
            } else if (entryTimes == times && entry.getKey().compareTo(topLanguage) < 0) {
                topLanguage = entry.getKey();
            }
        }

        languageData = new PairData(topLanguage, times);
    }

    /**
     * Merges keyword occurrence data from all partial databases
     */
    public void mergeKeyWords() {
        for (var db : partialDbs) {
            for (var entry : db.getKeyWordsOccurences().entrySet()) {
                keyWordsOccurences.merge(entry.getKey(), entry.getValue(), Integer::sum);
            }
        }

        for (var entry : keyWordsOccurences.entrySet()) {
            keywordsData.add(new PairData(entry.getKey(), entry.getValue()));
        }

        generateTopKeyWord();
    }

    /**
     * Determines the most frequently occurring keyword
     */
    private void generateTopKeyWord() {
        String kw = "";
        int times = -1;

        for (var entry : keywordsData) {
            if (entry.times() > times) {
                kw = entry.data();
                times = entry.times();
            } else if (entry.times() == times && entry.data().compareTo(kw) < 0) {
                kw = entry.data();
            } else if (entry.times() < times) {
                break;
            }
        }
        kwData = new PairData(kw, times);
    }

    /**
     * Merges the most recent article information from all partial databases
     */
    public void mergeMostRecentArticle() {
        List<NewsArticle> articlesToMerge = new ArrayList<>();

        partialDbs.forEach(db -> {
            if (db.getMostRecentArticle() != null) {
                articlesToMerge.add(db.getMostRecentArticle());
            }
        });

        Collections.sort(articlesToMerge);
        mostRecentArticle = articlesToMerge.getFirst();
    }

    /**
     * Merges author occurrence data from all partial databases
     */
    public void mergeAuthor() {
        Map<String, Integer> authorOcc = new HashMap<>();

        for (var db : partialDbs) {
            for (var entry : db.getAuthorOccurences().entrySet()) {
                authorOcc.merge(entry.getKey(), entry.getValue(), Integer::sum);
            }
        }

        generateBestAuthor(authorOcc);
    }

    /**
     * Determines the most prolific author based on article count
     *
     * @param authorOcc the map of author names to their article counts
     */
    private void generateBestAuthor(Map<String, Integer> authorOcc) {
        String bestAuthor = "";
        int articles = -1;

        for (var entry : authorOcc.entrySet()) {
            if (entry.getValue() > articles) {
                articles = entry.getValue();
                bestAuthor = entry.getKey();
            } else if (entry.getValue() == articles && bestAuthor.compareTo(entry.getKey()) > 0) {
                bestAuthor = entry.getKey();
            }
        }

        authorData = new PairData(bestAuthor, articles);
    }

    /**
     * Calculates the number of duplicate articles that were removed
     *
     * @return the number of articles removed as duplicates
     */
    public int getDuplicatesRemoved() {
        return counter.intValue() - articles.size();
    }

    /**
     * Increments the total article counter by the specified amount in a thread-safe manner.
     *
     * @param n the number to add to the article counter
     */
    public void incrementArts(int n) {
        counter.addAndGet(n);
    }

    public Map<String, List<String>> getCategoryToArticle() {
        return categoryToArticle;
    }

    public Map<String, List<String>> getLanguageToArticle() {
        return languageToArticle;
    }

    public NewsArticle getMostRecentArticle() {
        return mostRecentArticle;
    }

    public PairData getLanguageData() {
        return languageData;
    }

    public Set<PairData> getKeywordsData() {
        return keywordsData;
    }

    public PairData getAuthorData() {
        return authorData;
    }

    public PairData getKwData() {
        return kwData;
    }

    public PairData getCategoryData() {
        return categoryData;
    }

    public Map<String, Integer> getUuidCount() {
        return uuidCount;
    }

    public Map<String, Integer> getTitleCount() {
        return titleCount;
    }

    public List<NewsArticle> getArticles() {
        return articles;
    }
}
