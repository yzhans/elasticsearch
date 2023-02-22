package cn.itcast.hotel;

import cn.itcast.hotel.pojo.Hotel;
import cn.itcast.hotel.pojo.HotelDoc;
import cn.itcast.hotel.service.IHotelService;
import com.alibaba.fastjson.JSON;
import org.apache.http.HttpHost;
import org.apache.lucene.search.TotalHits;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightField;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.search.suggest.Suggest;
import org.elasticsearch.search.suggest.SuggestBuilder;
import org.elasticsearch.search.suggest.SuggestBuilders;
import org.elasticsearch.search.suggest.completion.CompletionSuggestion;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.util.CollectionUtils;

import javax.annotation.Resource;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@SpringBootTest
public class HotelSearchTest {
    private RestHighLevelClient restHighLevelClient;

    @Resource
    private IHotelService iHotelService;

    @Test
    void testMatch() throws IOException {
        SearchRequest request = new SearchRequest("hotel");
        request.source().query(QueryBuilders.matchQuery("all","如家"));
        SearchResponse search = restHighLevelClient.search(request, RequestOptions.DEFAULT);
        handleResponse(search);
    }

    @Test
    void testMatchAll() throws IOException {
        SearchRequest request = new SearchRequest("hotel");
        request.source().query(QueryBuilders.matchAllQuery());
        SearchResponse search = restHighLevelClient.search(request, RequestOptions.DEFAULT);
        handleResponse(search);
    }

    @Test
    void testHighlight() throws IOException {
        SearchRequest request = new SearchRequest("hotel");
        request.source().query(QueryBuilders.matchQuery("all", "如家"));
        request.source().highlighter(new HighlightBuilder().field("name").requireFieldMatch(false));
        SearchResponse search = restHighLevelClient.search(request, RequestOptions.DEFAULT);
        handleResponse(search);
    }

    @Test
    void testBool() throws IOException {
        SearchRequest request = new SearchRequest("hotel");
        BoolQueryBuilder boolQuery = QueryBuilders.boolQuery();
        boolQuery.must(QueryBuilders.termQuery("city", "上海"));
        request.source().query(boolQuery);
        SearchResponse search = restHighLevelClient.search(request, RequestOptions.DEFAULT);
        handleResponse(search);
    }

    @Test
    void testPageAndSort() throws IOException {
        SearchRequest request = new SearchRequest("hotel");
        request.source().query(QueryBuilders.matchAllQuery());
        request.source().sort("price", SortOrder.DESC);
        request.source().from(0).size(5);
        SearchResponse search = restHighLevelClient.search(request, RequestOptions.DEFAULT);
        handleResponse(search);
    }

    private static void handleResponse(SearchResponse search) {
        SearchHits hits = search.getHits();

        TotalHits hitsTotalHits = hits.getTotalHits();
        System.out.println("hitsTotalHits = " + hitsTotalHits);

        SearchHit[] searchHits = hits.getHits();

        for (SearchHit hit : searchHits) {
            String s = hit.getSourceAsString();

            HotelDoc hotelDoc = JSON.parseObject(s, HotelDoc.class);
            Map<String, HighlightField> highlightFields = hit.getHighlightFields();
            if (!CollectionUtils.isEmpty(highlightFields)) {
                HighlightField highlightField = highlightFields.get("name");
                if (highlightField != null) {
                    String string = highlightField.getFragments()[0].toString();
                    hotelDoc.setName(string);
                }
            }
            System.out.println(hotelDoc);
        }
    }

    @Test
    void testSuggest() throws IOException {
        SearchRequest request = new SearchRequest("hotel");
        request.source().suggest(new SuggestBuilder().addSuggestion(
                "suggestions",
                SuggestBuilders.completionSuggestion("suggestion")
                        .prefix("hz")
                        .skipDuplicates(true)
                        .size(10)
        ));
        SearchResponse response = restHighLevelClient.search(request, RequestOptions.DEFAULT);
        Suggest suggest = response.getSuggest();
        CompletionSuggestion suggestions = suggest.getSuggestion("suggestions");
        for (CompletionSuggestion.Entry.Option option : suggestions.getOptions()) {
            String text = option.getText().toString();
            System.out.println(text);
        }
    }

    @BeforeEach
    void setUp() {
        this.restHighLevelClient = new RestHighLevelClient(RestClient.builder(
                HttpHost.create("http://110.41.137.10:9200")
        ));
    }

    @AfterEach
    void tearDown() throws IOException {
        this.restHighLevelClient.close();
    }
}
