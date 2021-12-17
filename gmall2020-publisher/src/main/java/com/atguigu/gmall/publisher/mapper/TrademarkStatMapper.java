package com.atguigu.gmall.publisher.mapper;
import org.apache.ibatis.annotations.Param;
import java.util.List;
import java.util.Map;
/**
 * 品牌统计接口
 */
public interface TrademarkStatMapper {
    public List<Map> selectTradeSum(@Param("start_date") String startDate ,
                                    @Param("end_date")String endDate,
                                    @Param("topN")int topN);
}
