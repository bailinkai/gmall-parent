package com.blk.gmallpublisher.controller;

import com.alibaba.fastjson.JSON;
import com.blk.gmallpublisher.service.PublisherService;
import org.apache.commons.lang.time.DateUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

@RestController
public class PublisherController {

    @Autowired
    PublisherService publisherService;

    @GetMapping("realtime-total")
    public String getDauTotal(@RequestParam("date") String date){


        List<Map> mapList = new ArrayList<>();

        Map dauTotalMap = new HashMap();
        dauTotalMap.put("id", "dau");
        dauTotalMap.put("name", "新增日活");
        int dauTotal = publisherService.getDauTotal(date);
        dauTotalMap.put("value", dauTotal);
        mapList.add(dauTotalMap);

        Map newMidMap = new HashMap();
        newMidMap.put("id", "new_mid");
        newMidMap.put("name", "新增设备");
        newMidMap.put("value", 233);
        mapList.add(newMidMap);

        return JSON.toJSONString(mapList);
    }

    @GetMapping("realtime-hour")
    public String getRealtimeHour(@RequestParam("id") String id,@RequestParam("date") String date){
        if("dau".equals(id)){
            //日活
            Map dauTDHour = publisherService.getDauHour(date);
            String yDaystring = getYDaystring(date);
            Map dauYDHour = publisherService.getDauHour(yDaystring);

            HashMap<String, Map> hourMap = new HashMap<>();

            hourMap.put("today", dauTDHour);
            hourMap.put("yesterday", dauYDHour);

            return JSON.toJSONString(hourMap);

        }
        return null;
    }

    public String getYDaystring (String today){
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");

        String yday = "";

        try {

            Date date = sdf.parse(today);
            Date addDays = DateUtils.addDays(date, -1);
            yday = sdf.format(addDays);

        } catch (ParseException e) {
            e.printStackTrace();
        }
        return yday;
    }

}
