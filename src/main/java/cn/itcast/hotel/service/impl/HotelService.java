package cn.itcast.hotel.service.impl;

import cn.itcast.hotel.mapper.HotelMapper;
import cn.itcast.hotel.pojo.Hotel;
import cn.itcast.hotel.pojo.HotelDoc;
import cn.itcast.hotel.pojo.PageResult;
import cn.itcast.hotel.pojo.RequestParams;
import cn.itcast.hotel.service.IHotelService;
import com.alibaba.fastjson.JSON;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.unit.DistanceUnit;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.functionscore.FunctionScoreQueryBuilder;
import org.elasticsearch.index.query.functionscore.ScoreFunctionBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.MultiBucketsAggregation;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.search.suggest.Suggest;
import org.elasticsearch.search.suggest.SuggestBuilder;
import org.elasticsearch.search.suggest.SuggestBuilders;
import org.elasticsearch.search.suggest.completion.CompletionSuggestion;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Service
public class HotelService extends ServiceImpl<HotelMapper, Hotel> implements IHotelService {
    @Resource
    private RestHighLevelClient restHighLevelClient;

    public static void main(String[] args) {
        System.out.println("hallo world");
    }
    @Override
    public PageResult search(RequestParams params){
        try {
            SearchRequest request = new SearchRequest("hotel");

            buildBasicQuery(params, request);

            int page = params.getPage();
            int size = params.getSize();

            if (params.getLocation() != null && !"".equals(params.getLocation())) {
                request.source().sort(SortBuilders
                        .geoDistanceSort("location", new GeoPoint(params.getLocation()))
                        .order(SortOrder.ASC)
                        .unit(DistanceUnit.KILOMETERS));
            }

            request.source().from((page - 1) * size).size(size);

            SearchResponse response = restHighLevelClient.search(request, RequestOptions.DEFAULT);

            return handleResponse(response);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Map<String, List<String>> filters(RequestParams params){
        try {
            SearchRequest request = new SearchRequest("hotel");
            buildBasicQuery(params, request);
            request.source().size(0);
            buildAggregation(params, request);
            SearchResponse response = restHighLevelClient.search(request, RequestOptions.DEFAULT);
            Aggregations aggregations = response.getAggregations();
            Map<String, List<String>> result = new HashMap<>();
            result.put("brand", getAggByNameList(aggregations,"BrandAgg"));
            result.put("city", getAggByNameList(aggregations,"CityAgg"));
            result.put("starName", getAggByNameList(aggregations,"starAgg"));
            return result;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public List<String> getSuggestions(String key) {
        try {
            SearchRequest request = new SearchRequest("hotel");
            request.source().suggest(new SuggestBuilder().addSuggestion(
                    "suggestions",
                    SuggestBuilders.completionSuggestion("suggestion")
                            .prefix(key)
                            .skipDuplicates(true)
                            .size(10)
            ));
            SearchResponse response = restHighLevelClient.search(request, RequestOptions.DEFAULT);
            Suggest suggest = response.getSuggest();
            CompletionSuggestion suggestions = suggest.getSuggestion("suggestions");
            return suggestions.getOptions().stream().map((option -> option.getText().toString())).collect(Collectors.toList());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static List<String> getAggByNameList(Aggregations aggregations,String aggName) {
        Terms Agg = aggregations.get(aggName);
        List<? extends Terms.Bucket> AggBuckets = Agg.getBuckets();
        return AggBuckets.stream().map(MultiBucketsAggregation.Bucket::getKeyAsString).collect(Collectors.toList());
    }

    private static void buildAggregation(RequestParams params, SearchRequest request) {
        request.source().aggregation(AggregationBuilders.terms("BrandAgg")
                .field("brand")
                .size(params.getSize()));
        request.source().aggregation(AggregationBuilders.terms("CityAgg")
                .field("city")
                .size(params.getSize()));
        request.source().aggregation(AggregationBuilders.terms("starAgg")
                .field("starName")
                .size(params.getSize()));
    }

    private static void buildBasicQuery(RequestParams params, SearchRequest request) {
        BoolQueryBuilder boolQuery = QueryBuilders.boolQuery();
        //关键字搜索
        String key = params.getKey();
        if (key == null || "".equals(key)) {
            boolQuery.must(QueryBuilders.matchAllQuery());
        } else {
            boolQuery.must(QueryBuilders.matchQuery("all", key));
        }

        if (params.getCity() != null && !"".equals(params.getCity())) {
            boolQuery.filter(QueryBuilders.termQuery("city", params.getCity()));
        }

        if (params.getBrand() != null && !"".equals(params.getBrand())) {
            boolQuery.filter(QueryBuilders.termQuery("brand", params.getBrand()));
        }

        if (params.getStarName() != null && !"".equals(params.getStarName())) {
            boolQuery.filter(QueryBuilders.termQuery("starName", params.getStarName()));
        }

        if (params.getMinPrice() != null && params.getMaxPrice() != null) {
            boolQuery.filter(QueryBuilders
                    .rangeQuery("price").gte(params.getMinPrice()).lte(params.getMaxPrice()));
        }

        FunctionScoreQueryBuilder functionScoreQuery =
                QueryBuilders.functionScoreQuery(
                        boolQuery,
                        new FunctionScoreQueryBuilder.FilterFunctionBuilder[]{
                                new FunctionScoreQueryBuilder.FilterFunctionBuilder(
                                        QueryBuilders.termQuery("isAD", true),
                                        ScoreFunctionBuilders.weightFactorFunction(10)
                                )
                        });
        request.source().query(functionScoreQuery);
    }

    private static PageResult handleResponse(SearchResponse search) {
        SearchHits hits = search.getHits();

        long total = hits.getTotalHits().value;

        SearchHit[] searchHits = hits.getHits();

        List<HotelDoc> lists = Arrays.stream(searchHits).map(element -> {
            String json = element.getSourceAsString();
            HotelDoc hotelDoc = JSON.parseObject(json, HotelDoc.class);
            Object[] sortValues = element.getSortValues();
            if (sortValues.length > 0) {
                hotelDoc.setDistance(sortValues[0]);
            }
            return hotelDoc;
        }).collect(Collectors.toList());

        return new PageResult(total,lists);
    }
}
