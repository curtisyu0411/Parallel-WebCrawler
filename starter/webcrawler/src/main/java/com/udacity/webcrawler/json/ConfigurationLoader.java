package com.udacity.webcrawler.json;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Objects;


/**
 * A static utility class that loads a JSON configuration file.
 */
public final class ConfigurationLoader {

  private final Path path;

  /**
   * Create a {@link ConfigurationLoader} that loads configuration from the given {@link Path}.
   */
  public ConfigurationLoader(Path path) {
    this.path = Objects.requireNonNull(path);
  }

  /**
   * Loads configuration from this {@link ConfigurationLoader}'s path
   *
   * @return the loaded {@link CrawlerConfiguration}.
   */
  public CrawlerConfiguration load() {
    try (Reader reader = Files.newBufferedReader(path)) {
      return read(reader);
    } catch (IOException e) {
      e.printStackTrace();
    }
    return null;
  }

  /**
   * Loads crawler configuration from the given reader.
   *
   * @param reader a Reader pointing to a JSON string that contains crawler configuration.
   * @return a crawler configuration
   */
  public static CrawlerConfiguration read(Reader reader) {
    // This is here to get rid of the unused variable warning.
    Objects.requireNonNull(reader);
    // TODO: Fill in this method
    ObjectMapper mapper = new com.fasterxml.jackson.databind.ObjectMapper();
    mapper.disable(com.fasterxml.jackson.core.JsonParser.Feature.AUTO_CLOSE_SOURCE);
    try {
      CrawlerConfiguration configuration = mapper.readValue(reader, CrawlerConfiguration.class);
      return configuration;
    } catch (IOException e) {
      e.printStackTrace();
    }
    return null;
  }
}
