package cn.itcast.hotel;

import cn.itcast.hotel.pojo.Hotel;
import cn.itcast.hotel.pojo.HotelDoc;
import cn.itcast.hotel.service.IHotelService;
import com.alibaba.fastjson.JSON;
import org.apache.http.HttpHost;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;

import javax.annotation.Resource;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

@SpringBootTest
public class HotelDocumentTest {
    private RestHighLevelClient restHighLevelClient;

    @Resource
    private IHotelService iHotelService;

    @Test
    void testYm() {
        List<String> list = new ArrayList<>();
        list.add("aa");
    }

    @Test
    void testAddDocument() throws IOException {
        Hotel hotel = iHotelService.getById(56227L);
        HotelDoc hotelDoc = new HotelDoc(hotel);

        IndexRequest request = new IndexRequest("hotel").id(hotelDoc.getId().toString());
        request.source(JSON.toJSONString(hotelDoc), XContentType.JSON);
        restHighLevelClient.index(request, RequestOptions.DEFAULT);
    }

    @Test
    void getDocument() throws IOException {
        GetRequest request = new GetRequest("hotel", "56227");
        GetResponse response = restHighLevelClient.get(request, RequestOptions.DEFAULT);

        String json = response.getSourceAsString();
        System.out.println(json);

        //HotelDoc hotelDoc = JSON.parseObject(json, HotelDoc.class);
        //System.out.println(hotelDoc);
    }

    @Test
    void Update() throws IOException {
        UpdateRequest request = new UpdateRequest("hotel", "56227");
        request.doc(
                "location","88.888888,99.999999"
        );
        restHighLevelClient.update(request, RequestOptions.DEFAULT);
    }

    @Test
    void deleteDocument() throws IOException {
        DeleteRequest request = new DeleteRequest("hotel", "56227");
        restHighLevelClient.delete(request, RequestOptions.DEFAULT);
    }

    @Test
    void BulkRequest() throws IOException {
        List<Hotel> hotels = iHotelService.list();

        List<HotelDoc> hotelDocs = hotels.stream().map(HotelDoc::new).collect(Collectors.toList());

        BulkRequest request = new BulkRequest();

        hotelDocs.forEach(y->{
            request.add(new IndexRequest("hotel")
                    .id(y.getId().toString())
                    .source(JSON.toJSONString(y),XContentType.JSON));
        });

        restHighLevelClient.bulk(request, RequestOptions.DEFAULT);
    }

    @Test
    void BulkDeleteRequest() throws IOException {

        BulkRequest request = new BulkRequest();

        iHotelService.list().forEach(y->{
            request.add(new DeleteRequest("hotel",y.getId().toString()));
        });

        restHighLevelClient.bulk(request, RequestOptions.DEFAULT);
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
