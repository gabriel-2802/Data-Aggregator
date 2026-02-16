package articles;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.util.List;
import java.util.Objects;

/**
 * Data class for articles (uses the necessary fields only)
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class NewsArticle implements Comparable<NewsArticle> {

    private String uuid;
    private String url;

    private String author;
    private String published;
    private String title;
    private String text;

    private String language;
    private List<String> categories;

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof NewsArticle that)) return false;
        return uuid.equals(that.uuid);
    }

    @Override
    public int hashCode() {
        return Objects.hash(uuid);
    }

    @Override
    public int compareTo(NewsArticle o) {
        int cmp = o.published.compareTo(this.published);
        if (cmp != 0) return cmp;
        return this.uuid.compareTo(o.uuid);
    }


    @Override
    public String toString() {
        return published + " " + url;
    }

    public NewsArticle() {

    }

    public String getUuid() {
        return uuid;
    }

    public void setUuid(String uuid) {
        this.uuid = uuid;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public String getAuthor() {
        return author;
    }

    public void setAuthor(String author) {
        this.author = author;
    }

    public String getPublished() {
        return published;
    }

    public void setPublished(String published) {
        this.published = published;
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public String getText() {
        return text;
    }

    public void setText(String text) {
        this.text = text;
    }

    public String getLanguage() {
        return language;
    }

    public void setLanguage(String language) {
        this.language = language;
    }

    public List<String> getCategories() {
        return categories;
    }

    public void setCategories(List<String> categories) {
        this.categories = categories;
    }
}
