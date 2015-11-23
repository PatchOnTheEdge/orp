package de.tuberlin.orp.master;

import akka.actor.ActorRef;
import akka.actor.Props;
import akka.actor.UntypedActor;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.util.Version;
import scala.concurrent.duration.Duration;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static de.tuberlin.orp.common.JsonReader.readJsonFromUrl;

/**
 * Created by Patch on 22.09.2015.
 * A Search Handler is an Actor that manages a Lucene Search Index.
 */
public class SearchHandler extends UntypedActor {
  private LoggingAdapter log = Logging.getLogger(getContext().system(), this);
  private  ActorRef itemHandler;
  private Directory dir;
  private IndexWriter indexWriter;
  private Analyzer analyzer;

  public SearchHandler(ActorRef itemHandler) {
    this.itemHandler = itemHandler;
    this.dir = new RAMDirectory();
    this.analyzer = new StandardAnalyzer(Version.LUCENE_46);
    IndexWriterConfig config = new IndexWriterConfig(Version.LUCENE_46, analyzer);
    try {
      this.indexWriter = new IndexWriter(dir, config);
    } catch (IOException e) {
      e.printStackTrace();
    }
    //Directory directory = FSDirectory.open("/tmp/testindex");
  }

  public static Props create(ActorRef itemHandler) {
    return Props.create(SearchHandler.class, () -> {
      return new SearchHandler(itemHandler);
    });
  }

  @Override
  public void preStart() throws Exception {
    log.info("Search Handler started.");


    getContext().system().scheduler().schedule(Duration.create(30, TimeUnit.SECONDS), Duration.create(5, TimeUnit.MINUTES), () -> {
      //Future<Object> itemsFuture = Patterns.ask(itemHandler, "getItems", 100);

      ArrayNode items = null;
      JsonNode json = null;

      try {
        //TODO change to message
        json = readJsonFromUrl("http://localhost:9001/articles");
        items = (ArrayNode) json.get("items");
      } catch (IOException e) {
        e.printStackTrace();
      }
      if (items != null) {
        try {
          Set<Document> docSet = new HashSet<>();

          for (JsonNode data : items) {
            String itemId = data.get("itemId").asText();
            JsonNode item = data.get("item");
            String title = item.get("title").asText();
            String text = item.get("text").asText();

            Document doc = new Document();
            doc.add(new Field("id", itemId, TextField.TYPE_STORED));
            doc.add(new Field("title", title, TextField.TYPE_STORED));
            doc.add(new Field("text", text, TextField.TYPE_STORED));
            docSet.add(doc);
          }
          indexWriter.addDocuments(docSet);
          log.info("Building new Index successful!");

        } catch (IOException e) {
          log.error("Indexing items failed!");
          e.printStackTrace();
        }
      } else {
        log.error("No items found!");
      }
    }, getContext().dispatcher());

    super.preStart();
  }

  @Override
  public void postStop() throws Exception {
    indexWriter.close();
    super.postStop();
  }

  @Override
  public void onReceive(Object message) throws Exception {
    if (message.equals("search")) {

    }
  }
}
/*
    // Now search the index:
    DirectoryReader ireader = DirectoryReader.open(directory);
    IndexSearcher isearcher = new IndexSearcher(ireader);
    // Parse a simple query that searches for "text":
    QueryParser parser = new QueryParser("fieldname", analyzer);
    Query query = parser.parse("text");
    ScoreDoc[] hits = isearcher.search(query, null, 1000).scoreDocs;
    assertEquals(1, hits.length);
    // Iterate through the results:
    for (int i = 0; i < hits.length; i++) {
      Document hitDoc = isearcher.doc(hits[i].doc);
      assertEquals("This is the text to be indexed.", hitDoc.get("fieldname"));
    }
    ireader.close();
    directory.close();*/

