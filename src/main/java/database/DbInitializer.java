package database;

import auxs.Utils;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;

/**
 * Initializes the database by loading article file paths and auxiliary data
 */
public class DbInitializer {
    /**
     * A record holding the file paths to auxiliary configuration files
     *
     * @param langFile the path to the languages file
     * @param categFile the path to the categories file
     * @param wordsFile the path to the linking words file
     */
    private record AuxiliaryFiles(String langFile, String categFile, String wordsFile) {}

    /**
     * Initializes the database with article files and auxiliary data
     *
     * @param newsFilesPath the path to the file containing the list of article files
     * @param additionalFilesPath the path to the file containing auxiliary file paths
     * @return a list of file paths to article JSON files to be processed
     * @throws IOException if any file reading operation fails
     */
    public List<String> initDb(String newsFilesPath, String additionalFilesPath) throws IOException {
        ConcurrentDb db = ConcurrentDb.getInstance();

        List<String> filesToRead = loadArticleList(newsFilesPath);
        AuxiliaryFiles aux = loadAuxiliaryFiles(additionalFilesPath);

        db.setLanguages(loadListFile(aux.langFile));
        db.setCategories(loadListFile(aux.categFile));
        db.setLinkingWords(loadListFile(aux.wordsFile));

        return filesToRead;
    }

    /**
     * Loads the list of article file paths from a configuration file
     *
     * @param path the path to the file containing the article file list
     * @return a list of resolved absolute paths to article files
     * @throws IOException if the file cannot be read or has unexpected format
     */
    private List<String> loadArticleList(String path) throws IOException {
        List<String> result = new ArrayList<>();

        try (BufferedReader br = new BufferedReader(new FileReader(path))) {
            int n = Integer.parseInt(br.readLine().trim());

            for (int i = 0; i < n; i++) {
                String line = br.readLine();
                if (line == null)
                    throw new IOException("Unexpected end of article file list.");

                String trimmed = line.trim();
                String resolved = Utils.replaceLastPathElement(path, trimmed);
                result.add(resolved);
            }
        }
        return result;
    }

    /**
     * Loads the paths to auxiliary files (languages, categories, and linking words)
     *
     * @param path the path to the file containing auxiliary file paths
     * @return an AuxiliaryFiles record containing the resolved paths to all auxiliary files
     * @throws IOException if the file cannot be read, has unexpected format, or doesn't contain exactly 3 files
     */
    private AuxiliaryFiles loadAuxiliaryFiles(String path) throws IOException {

        try (BufferedReader br = new BufferedReader(new FileReader(path))) {
            int n = Integer.parseInt(br.readLine().trim());
            if (n != 3)
                throw new IOException("Expected exactly 3 auxiliary files");

            String lang = br.readLine().trim().replaceFirst("^\\./", "");
            String categ = br.readLine().trim().replaceFirst("^\\./", "");
            String words = br.readLine().trim().replaceFirst("^\\./", "");

            String lanPath = Utils.replaceLastPathElement(path, lang);
            String categPath = Utils.replaceLastPathElement(path, categ);
            String wordsPath = Utils.replaceLastPathElement(path, words);

            return new AuxiliaryFiles(lanPath, categPath, wordsPath);
        }
    }

    /**
     * Loads a set of strings from a text file
     *
     * @param path the path to the file to load
     * @return a set of strings read from the file (excluding the count line), or an empty set if an error occurs
     */
    private Set<String> loadListFile(String path) {
        Set<String> set = new HashSet<>();

        try (BufferedReader br = new BufferedReader(new FileReader(path))) {
            int count = Integer.parseInt(br.readLine().trim());

            for (int i = 0; i < count; i++) {
                String line = br.readLine();
                if (line != null) {
                    set.add(line);
                }
            }
        } catch (IOException e) {
            System.err.println(e.getMessage());
        }
        return set;
    }
}
