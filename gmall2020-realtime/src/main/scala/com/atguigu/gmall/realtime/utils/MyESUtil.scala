package com.atguigu.gmall.realtime.utils

import com.atguigu.gmall.realtime.bean.DauInfo
import io.searchbox.client.config.HttpClientConfig
import io.searchbox.client.{JestClient, JestClientFactory}
import io.searchbox.core.{Bulk, BulkResult, DocumentResult, Get, Index, Search, SearchResult}
import org.elasticsearch.index.query.{BoolQueryBuilder, MatchQueryBuilder, TermQueryBuilder}
import org.elasticsearch.search.builder.SearchSourceBuilder
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder
import org.elasticsearch.search.sort.SortOrder

import java.util

/**
 * 程序中操作 ES 的工具类
 */
object MyESUtil {
  //
  private var factory: JestClientFactory = null;

  def build(): Unit = {
    factory = new JestClientFactory
    //构造者模式
    factory.setHttpClientConfig(new HttpClientConfig.Builder("http://hadoop102:9200")
      .multiThreaded(true)
      .maxTotalConnection(20)
      .connTimeout(10000)
      .readTimeout(1000)
      .build())
  }

  def getClient: JestClient = {
    if (factory == null) build();
    factory.getObject
  }

  //向ES中插入数据
  def putIndex(): Unit = {
    //建立连接
    val jest: JestClient = getClient
    //Builder 中的参数，底层会转换为 Json 格式字符串，所以我们这里封装 Document 为样例类
    //当然也可以直接传递 json
    val actorNameList = new util.ArrayList[String]()
    actorNameList.add("zhangsan")
    actorNameList.add("lisi")
    //构造者模式
    val index = new Index.Builder(Movie("100", "天龙八部", actorNameList))
      .index("movie_index_5")
      .`type`("movie")
      .id("1")
      .build()
    //execute 的参数类型为 Action， Action 是接口类型，不同的操作有不同的实现类，添加的实现类为 Index
    val documentResult: DocumentResult = jest.execute(index)
    println(documentResult.getVersion)
    //关闭连接
    jest.close()
  }

  //从ES中查询数据
  def queryIndex(): Unit = {
    //建立连接
    val jest: JestClient = getClient
    //查询常用有两个实现类 Get 通过 id 获取单个 Document，以及 Search 处理复杂查询
    val query =
      """
        |{
        | "query": {
        | "bool": {
        | "must": [
        | {"match": {
        | "name": "红"
        | }}
        | ],
        | "filter": [
        | {"term": { "actorList.name.keyword": "张涵予"}}
        | ]
        | }
        | },
        | "from": 0,
        | "size": 20,
        | "sort": [
        | {
        | "doubanScore": {
        | "order": "desc"
        | }
        | }
        | ],
        | "highlight": {
        | "fields": {
        | "name": {}
        | }
        | }
        |}
        """.stripMargin
        println(query)
    val search: Search  = new Search.Builder(query).addIndex("movie_chn_1").build()
    val result:SearchResult = jest.execute(search)
    //获取命中的结果 sourceType: 对命中的数据进行封装, 因为是Json,所以我们用map封装,
    // 注意, 一定得是Java的Map类型
    val rsList: util.List[SearchResult#Hit[util.Map[String, Any], Void]] = result.getHits(classOf[util.Map[String, Any]])
    //将java转换为Scala集合, 方便操作
    import scala.collection.JavaConverters._
    val list: List[util.Map[String, Any]] = rsList.asScala.map(_.source).toList
    println(list.mkString("\n"))
    jest.close()
  }
  def queryIndex2(): Unit = {
    //建立连接
    val jest: JestClient = getClient
    //查询常用有两个实现类 Get 通过 id 获取单个 Document，以及 Search 处理复杂查询
    //通过SearchSourceBuilder构建查询语句
    val sourceBuilder:SearchSourceBuilder  = new SearchSourceBuilder
    //bool片段
    val boolQueryBuilder = new BoolQueryBuilder()
    boolQueryBuilder.must(new MatchQueryBuilder("name","红"))
    boolQueryBuilder.filter(new TermQueryBuilder("actorList.name.keyword","张涵予"))
    sourceBuilder.query(boolQueryBuilder)
    //from,size片段
    sourceBuilder.from(0)
    sourceBuilder.size(20)
    //排序片段
    sourceBuilder.sort("doubanScore",SortOrder.DESC)
    //高亮片段
    sourceBuilder.highlighter(new HighlightBuilder().field("name"))
    //json版本的query数据
    var query =
      """
        |{
        | "query": {
        | "bool": {
        | "must": [
        | {"match": {
        | "name": "红"
        | }}
        | ],
        | "filter": [
        | {"term": { "actorList.name.keyword": "张涵予"}}
        | ]
        | }
        | },
        | "from": 0,
        | "size": 20,
        | "sort": [
        | {
        | "doubanScore": {
        | "order": "desc"
        | }
        | }
        | ],
        | "highlight": {
        | "fields": {
        | "name": {}
        | }
        | }
        |}
        """.stripMargin

    //最后构造json数据
    query = sourceBuilder.toString()

    println(query)
    val search: Search  = new Search.Builder(query).addIndex("movie_chn_1").build()
    val result:SearchResult = jest.execute(search)
    //获取命中的结果 sourceType: 对命中的数据进行封装, 因为是Json,所以我们用map封装,
    // 注意, 一定得是Java的Map类型
    val rsList: util.List[SearchResult#Hit[util.Map[String, Any], Void]] = result.getHits(classOf[util.Map[String, Any]])
    //将java转换为Scala集合, 方便操作
    import scala.collection.JavaConverters._
    val list: List[util.Map[String, Any]] = rsList.asScala.map(_.source).toList
    println(list.mkString("\n"))
    jest.close()
  }

  //根据文档的id,从Es中查询一条记录
  def queryIndexById():Unit = {
    val jestClient = getClient
    val get:Get = new Get.Builder("movie_index_5", "1").build()
    val result: DocumentResult = jestClient.execute(get)
    //遍历result, 打印数据
    println(result.getJsonString)
    jestClient.close()
  }
  case class Movie(id: String, movie_name: String, actorNameList: java.util.List[String]) {}

  //向ES中批量插入数据
  def bulkInsert(sourceList:List[(String,DauInfo)],indexName:String):Unit = {
    if(sourceList!=null && sourceList.nonEmpty){
      //获取操作对象
      var jest = getClient//构造批次操作
      val bulkBuild = new Bulk.Builder
      //对批量操作的数据进行遍历
      for ((mid,source) <-sourceList){
        val index = new Index.Builder(source)
          .id(mid)//幂等性,在批量向 ES 写数据的时候，指定 Index 的 id 即可
          .index(indexName)
          .`type`("_doc")
          .build()

        //将每条数据添加到批量操作中
        bulkBuild.addAction(index)
      }
      //Bulk是Action的实现类,主要实现批量操作
      val bulk = bulkBuild.build()
      //执行批量操作, 获取执行结果
      val result = jest.execute(bulk)
      //通过执行结果,获取批量插入的数据
      val items: util.List[BulkResult#BulkResultItem] = result.getItems
      println("保存到ES "+items.size() + " 条数")
      jest.close()
    }
  }
  def main(args: Array[String]): Unit = {
//    putIndex()
//    queryIndex()
//    queryIndex2()
    queryIndexById()
  }
}
