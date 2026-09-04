import io.ketebe.KetebeClient;
import io.ketebe.QueryRequest;
import io.ketebe.QueryResponse;
import io.ketebe.RecordId;
import io.ketebe.RecordUpsert;
import java.util.List;

public final class Quickstart {
    public static void main(String[] args) {
        KetebeClient client = new KetebeClient("http://127.0.0.1:8080");
        client.upsertRecord("docs", RecordId.string("example"), new RecordUpsert(List.of(1.0, 0.0), null));
        QueryResponse result = client.query("docs", new QueryRequest().vector(List.of(1.0, 0.0)).topK(5).execution("exact"));
        result.hits().forEach(hit -> System.out.println(hit.id() + " score=" + hit.score()));
    }
}
