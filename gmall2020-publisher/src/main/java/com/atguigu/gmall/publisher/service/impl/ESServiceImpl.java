package com.atguigu.gmall.publisher.service.impl;

import com.atguigu.gmall.publisher.service.ESService;
import io.searchbox.action.Action;
import io.searchbox.client.JestResult;
import io.searchbox.core.Search;
import io.searchbox.core.SearchResult;
import io.searchbox.core.search.aggregation.TermsAggregation;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import io.searchbox.client.JestClient;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
public class ESServiceImpl implements ESService {
    @Autowired
    JestClient jestClient;
    @Override
    public Long getDauTotal(String date) {
        //构造者模式
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(new MatchAllQueryBuilder());
        String query = searchSourceBuilder.toString();
        String indexName = "gmall2020_dau_info_"+date +"-query";
        Search search = new Search.Builder(query).addIndex(indexName).addType("_doc").build();
        //返回的结果
        Long total = 0L;
        try {
            SearchResult searchResult = jestClient.execute(search);
            if(searchResult.getTotal()!=null){
                total = searchResult.getTotal();
            }
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException("查询ES异常");
        }
        return total;
    }

    @Override
    public Long getNewMidTotal(String date) {
        //TODO:新增设备
        return 666L;
    }
    //日活的分时查询
    @Override
    public Map getDauHour(String date) {
        //构造者模式
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        //聚合
        TermsAggregationBuilder aggBuilder = AggregationBuilders.terms("groupby_hr").field("hr").size(24);
        searchSourceBuilder.aggregation(aggBuilder);
        String indexName = "gmall2020_dau_info_"+date+"-query";
        Search search = new Search.Builder(searchSourceBuilder.toString()).addIndex(indexName).addType("_doc").build();
        try {
            SearchResult searchResult = jestClient.execute(search);
            //封装返回的结果
            Map<String, Long> aggMap = new HashMap<>();
            //判断es的返回数据内的groupby_hr(这是自定义的名称)是否null
            if(searchResult.getAggregations().getTermsAggregation("groupby_hr")!=null){
                List<TermsAggregation.Entry> buckets = searchResult.getAggregations().getTermsAggregation("groupby_hr").getBuckets();
                //遍历buckets, 将各项数据保存到map结构
                for (TermsAggregation.Entry bucket : buckets) {
                    aggMap.put(bucket.getKey(),bucket.getCount());// key-value
                }
            }
            return aggMap;
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException("ES查询异常");
        }
    }
}
