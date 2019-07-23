package com.blk.gmallpublisher.mapper;

import java.util.List;
import java.util.Map;

public interface DauMapper {

    int getDauTotal(String data);

    List<Map> getDauHour(String date);

}
