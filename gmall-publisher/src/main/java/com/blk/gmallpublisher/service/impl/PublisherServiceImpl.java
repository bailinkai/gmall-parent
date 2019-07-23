package com.blk.gmallpublisher.service.impl;

import com.blk.gmallpublisher.mapper.DauMapper;
import com.blk.gmallpublisher.service.PublisherService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
public class PublisherServiceImpl implements PublisherService {

    @Autowired
    DauMapper dauMapper;


    @Override
    public int getDauTotal(String data) {

        int dauTotal = dauMapper.getDauTotal(data);
        return dauTotal;
    }

    @Override
    public Map getDauHour(String date) {

        HashMap hashMap = new HashMap<>();
        List<Map> mapList = dauMapper.getDauHour(date);

        for (Map map : mapList) {
            String loghour = (String) map.get("LOGHOUR");
            Long ct = (Long) map.get("CT");
            hashMap.put(loghour, ct);
        }

        return hashMap;
    }
}
