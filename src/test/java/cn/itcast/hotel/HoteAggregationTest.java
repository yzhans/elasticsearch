package cn.itcast.hotel;

import org.apache.http.HttpHost;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;

public class HoteAggregationTest {
    private RestHighLevelClient restHighLevelClient;

    @Test
    void testAggregation() throws IOException {
        SearchRequest request = new SearchRequest("hotel");
        request.source().size(0);
        request.source().aggregation(AggregationBuilders.terms("BrandAgg")
                .field("brand")
                .size(10));
        SearchResponse response = restHighLevelClient.search(request, RequestOptions.DEFAULT);

        Aggregations aggregations = response.getAggregations();

        Terms brandAgg = aggregations.get("BrandAgg");

        List<? extends Terms.Bucket> list = brandAgg.getBuckets();
        for (Terms.Bucket bucket : list) {
            String key = bucket.getKeyAsString();
            System.out.println(key);
        }
    }

    @Test
    void testAll() throws IOException {
        SearchRequest request = new SearchRequest("hotel");
        request.source().size(0);
        request.source().aggregation(AggregationBuilders.terms("BrandAgg")
                .field("brand")
                .size(100));
        request.source().aggregation(AggregationBuilders.terms("CityAgg")
                .field("city")
                .size(100));
        request.source().aggregation(AggregationBuilders.terms("starAgg")
                .field("starName")
                .size(100));
        SearchResponse response = restHighLevelClient.search(request, RequestOptions.DEFAULT);

        Aggregations aggregations = response.getAggregations();

        Terms brandAgg = aggregations.get("BrandAgg");
        Terms cityAgg = aggregations.get("CityAgg");
        Terms starName = aggregations.get("starAgg");

        List<? extends Terms.Bucket> list = brandAgg.getBuckets();
        List<? extends Terms.Bucket> cityAggBuckets = cityAgg.getBuckets();
        List<? extends Terms.Bucket> starNameBuckets = starName.getBuckets();
        for (Terms.Bucket bucket : list) {
            String key = bucket.getKeyAsString();
            System.out.println(key);
        }
        for (Terms.Bucket cityAggBucket : cityAggBuckets) {
            String key = cityAggBucket.getKeyAsString();
            System.out.println(key);
        }
        for (Terms.Bucket starNameBucket : starNameBuckets) {
            String key = starNameBucket.getKeyAsString();
            System.out.println(key);
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
