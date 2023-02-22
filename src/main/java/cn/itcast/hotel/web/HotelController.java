package cn.itcast.hotel.web;

import cn.itcast.hotel.pojo.PageResult;
import cn.itcast.hotel.pojo.RequestParams;
import cn.itcast.hotel.service.IHotelService;
import org.springframework.web.bind.annotation.*;

import javax.annotation.Resource;
import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/hotel")
public class HotelController {

    @Resource
    private IHotelService iHotelService;

    @PostMapping("/list")
    public PageResult search(@RequestBody RequestParams params) {
        return iHotelService.search(params);
    }

    @PostMapping("/filters")
    public Map<String, List<String>> getFilters(@RequestBody RequestParams params) {
        return iHotelService.filters(params);
    }

    @GetMapping("/suggestion")
    public List<String> getSuggestions(@RequestParam("key") String key) {
        return iHotelService.getSuggestions(key);
    }
}
