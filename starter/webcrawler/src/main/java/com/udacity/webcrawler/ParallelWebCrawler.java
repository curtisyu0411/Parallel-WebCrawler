package com.udacity.webcrawler;

import com.udacity.webcrawler.json.CrawlResult;
import com.udacity.webcrawler.parser.PageParser;
import com.udacity.webcrawler.parser.PageParserFactory;
import org.checkerframework.checker.units.qual.C;

import javax.inject.Inject;
import javax.inject.Provider;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * A concrete implementation of {@link WebCrawler} that runs multiple threads on a
 * {@link ForkJoinPool} to fetch and process multiple web pages in parallel.
 */
final class ParallelWebCrawler implements WebCrawler {
  private final Clock clock;
  private final Duration timeout;
  private final int popularWordCount;
  private final ForkJoinPool pool;
  private final int maxDepth;
  private final List<Pattern> ignoredUrls;
  @Inject private com.udacity.webcrawler.parser.PageParserFactory factory;

  @Inject
  ParallelWebCrawler(
      Clock clock,
      @Timeout Duration timeout,
      @PopularWordCount int popularWordCount,
      @TargetParallelism int threadCount,
      @MaxDepth int maxDepth,
      @IgnoredUrls List<Pattern> ignoredUrls,
      PageParserFactory factory) {
    this.clock = clock;
    this.timeout = timeout;
    this.popularWordCount = popularWordCount;
    this.maxDepth = maxDepth;
    this.pool = new ForkJoinPool(Math.min(threadCount, getMaxParallelism()));
    this.ignoredUrls = ignoredUrls;
    this.factory = factory;
  }

  @Override
  public CrawlResult crawl(List<String> startingUrls) {
    Instant deadline = clock.instant().plus(timeout);
    Map<String, Integer> counts = new ConcurrentHashMap<>();
    Set<String> visitedUrls = new HashSet<>();
    for (String url: startingUrls) {
      pool.invoke(new CrawlTask(maxDepth, url, deadline, counts, ignoredUrls, visitedUrls, clock, factory));
    }


    if (counts.isEmpty()) {
      return new CrawlResult.Builder()
              .setWordCounts(counts)
              .setUrlsVisited(visitedUrls.size())
              .build();
    }

    return new CrawlResult.Builder()
            .setWordCounts(WordCounts.sort(counts, popularWordCount))
            .setUrlsVisited(visitedUrls.size())
            .build();

  }

  @Override
  public int getMaxParallelism() {
    return Runtime.getRuntime().availableProcessors();
  }

  private static  final class CrawlTask extends RecursiveAction {
    private int _depth;
    private String _url;
    private Instant _deadline;
    private Map<String, Integer> _counts;
    private Set<String> _visited;
    private List<Pattern> _ignored;
    private Clock _clock;
    private com.udacity.webcrawler.parser.PageParserFactory _factory;

    private CrawlTask(int _depth,
                      String _url,
                      Instant _deadline,
                      Map<String, Integer> _counts,
                      List<Pattern> _ignored,
                      Set<String> _visited,
                      Clock _clock,
                      PageParserFactory factory) {
      this._depth = _depth;
      this._deadline = _deadline;
      this._url = _url;
      this._counts = _counts;
      this._ignored =_ignored;
      this._visited = _visited;
      this._clock = _clock;
      this._factory = factory;
    }

    @Override
    protected void compute() {
      if (_depth == 0 || _clock.instant().isAfter(_deadline)) {
        return;
      }
      for (Pattern pattern : _ignored) {
        if (pattern.matcher(_url).matches()) {
          return;
        }
      }
      if (_visited.contains(_url)) {
        return;
      }
      _visited.add(_url);
      PageParser.Result result = _factory.get(_url).parse();
      for (Map.Entry<String, Integer> e : result.getWordCounts().entrySet()) {
        if (_counts.containsKey(e.getKey())) {
          _counts.put(e.getKey(), e.getValue() + _counts.get(e.getKey()));
        } else {
          _counts.put(e.getKey(), e.getValue());
        }
      }
      List<CrawlTask> tasks = result.getLinks()
              .parallelStream()
              .map(link -> new CrawlTask(_depth - 1, link,_deadline,_counts, _ignored, _visited, _clock, _factory))
              .collect(Collectors.toList());
      invokeAll(tasks);
    }
  }
}
