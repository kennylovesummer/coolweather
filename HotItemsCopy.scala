import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util
import java.util.Properties

import bean.PreciseAccumulator
import com.alibaba.fastjson.JSON
import org.apache.flink.api.common.functions.{AggregateFunction, RuntimeContext}
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.state._
import org.apache.flink.api.java.tuple.{Tuple, Tuple1}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.datastream.DataStreamSink
import org.apache.flink.streaming.api.functions.{KeyedProcessFunction, ProcessFunction}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.{ProcessWindowFunction, WindowFunction}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.{ContinuousEventTimeTrigger, ContinuousProcessingTimeTrigger, Trigger, TriggerResult}
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.connectors.elasticsearch.{ElasticsearchSinkFunction, RequestIndexer}
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.util.Collector
import org.apache.http.HttpHost
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.client.Requests
import org.roaringbitmap.longlong.Roaring64NavigableMap
import redis.clients.jedis.Jedis

import scala.collection.mutable.ListBuffer
import scala.util.parsing.json.JSONObject

/**
  * Copyright (c) 2018-2028 尚硅谷 All Rights Reserved
  *
  * Project: UserBehaviorAnalysis
  * Package:
  * Version: 1.0
  *
  * Created by wushengran on 2019/6/11 11:24
  */
// 输入数据样例类
case class UserBehavior( userId: Long, itemId: Long, categoryId: Int, behavior: String, timestamp: Long )
// 输出数据样例类
case class ItemViewCount( itemId: Long, windowEnd: Long, count: Long )
case class ItemViewCount2( itemId: Long, windowEnd: Long, count: Long )

object HotItemsCopy {
 // var roaring64Navigable = new Roaring64NavigableMap
  def main(args: Array[String]): Unit = {

    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "localhost:9092")
    properties.setProperty("group.id", "consumer-group")
    properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("auto.offset.reset", "latest")

    // 创建一个env
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    // 显式地定义Time类型
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

     val hosts = new util.ArrayList[HttpHost]()
    hosts.add(new HttpHost("localhost",9200))

//    private final List<HttpHost> httpHosts;
//    private final ElasticsearchSinkFunction<T> elasticsearchSinkFunction;
//    private Map<String, String> bulkRequestsConfig = new HashMap();
//    private ActionRequestFailureHandler failureHandler = new NoOpFailureHandler();

    val esSinkBuild = new ElasticsearchSink.Builder[UserBehavior](hosts,new ElasticsearchSinkFunction[UserBehavior]() {
     def creatIndexFunction(t:UserBehavior):IndexRequest ={
     // val str = JSON.toJSONString(t)
       val str=""
       Requests.indexRequest().index("index-student").`type`("student").source(str)
      }
      override def process(t: UserBehavior, runtimeContext: RuntimeContext, requestIndexer: RequestIndexer) = {
        requestIndexer.add(creatIndexFunction(t))
      }
    }

    )

    val stream = env
     .readTextFile("E:\\037_Flink项目\\037_Flink项目\\UserBehaviorAnalysis\\UserBehaviorAnalysis\\HotItemsAnalysis\\src\\main\\resources\\UserBehavior.csv")
     // .addSource( new FlinkKafkaConsumer[String]("hotitems", new SimpleStringSchema(), properties) )
      .map(line => {
        val linearray = line.split(",")
        UserBehavior( linearray(0).toLong, linearray(1).toLong, linearray(2).toInt, linearray(3), linearray(4).toLong )
      })
      // 指定时间戳和watermark
      .assignAscendingTimestamps(_.timestamp * 1000)
      .filter(_.behavior == "pv")
      .keyBy("itemId")
        .timeWindow(Time.hours(1))
       .trigger(new MyTriggerTwo(7000L))
        .process(new UvcountWithBloom)
//      .timeWindow(Time.hours(1), Time.minutes(5))

//
//
      .process(new BollomProcess)
     // .aggregate( new CountAgg(), new WindowResultFunction() )
//      .keyBy("windowEnd")
//      .process( new TopNHotItems(3))
     //   stream.addSink(esSinkBuild.build())
      .print()

    // 调用execute执行任务
    env.execute("Hot Items Job")
  }

  //自定义myTrigger
  class MyTrigger() extends  Trigger[UserBehavior,TimeWindow]{
    var count = 0L
    override def onEventTime(l: Long, w: TimeWindow, triggerContext: Trigger.TriggerContext): TriggerResult = TriggerResult.CONTINUE

    override def onProcessingTime(l: Long, w: TimeWindow, triggerContext: Trigger.TriggerContext): TriggerResult = TriggerResult.CONTINUE

    override def clear(w: TimeWindow, triggerContext: Trigger.TriggerContext): Unit = {}

    override def onElement(t: UserBehavior, l: Long, w: TimeWindow, triggerContext: Trigger.TriggerContext): TriggerResult = {
      count+=1L
      if(count >100L){
        TriggerResult.FIRE_AND_PURGE
      }else
      {
        TriggerResult.CONTINUE
      }
    }
  }

  //自定义myTrigger
  class MyTriggerTwo(interval: Long) extends  Trigger[UserBehavior,TimeWindow]{
    val stateDesc = new ValueStateDescriptor[Long]("fireTimestamp", classOf[Long])
   // private var fireTimestamp:ValueState[Long]= _
    override def onEventTime(time: Long, window: TimeWindow, triggerContext: Trigger.TriggerContext): TriggerResult = {
      val fireTimestamp = triggerContext.getPartitionedState(stateDesc)

      if (fireTimestamp.value() == time) {
        fireTimestamp.clear()
        fireTimestamp.update(time + interval)
        triggerContext.registerProcessingTimeTimer(time + interval)
         TriggerResult.FIRE_AND_PURGE
      } else if (window.maxTimestamp == time) {
        TriggerResult.FIRE_AND_PURGE
      }else
      {
        TriggerResult.CONTINUE
      }
    }

    override def onProcessingTime(l: Long, w: TimeWindow, triggerContext: Trigger.TriggerContext): TriggerResult = TriggerResult.CONTINUE

    override def clear(w: TimeWindow, triggerContext: Trigger.TriggerContext): Unit = {}

    override def onElement(t: UserBehavior, l: Long, w: TimeWindow, triggerContext: Trigger.TriggerContext): TriggerResult = {

   //   val timestamp =triggerContext.getCurrentWatermark
      val timestamp =triggerContext.getCurrentProcessingTime

       val fireTimestamp = triggerContext.getPartitionedState(stateDesc)

      if (fireTimestamp.value() == null) {
        val start = timestamp - (timestamp % interval)
        val nextFireTimestamp = start + interval
        triggerContext.registerEventTimeTimer(nextFireTimestamp)
        fireTimestamp.update(nextFireTimestamp)
         TriggerResult.CONTINUE
      }else
      {
        TriggerResult.CONTINUE
      }
    }
  }

  class UvcountWithBloom() extends ProcessWindowFunction[UserBehavior,ItemViewCount,Tuple,TimeWindow]{

    lazy val jedist = new Jedis("localhost",6379)
    lazy val bloom = new Bloom(1<<29)


    override def process(key: Tuple, context: Context, elements: Iterable[UserBehavior], out: Collector[ItemViewCount]): Unit = {
      val StorKey = context.window.getEnd.toString
      var count = 0L
      if(jedist.hget("count",StorKey)!=null){
        count = jedist.hget("count",StorKey).toLong
      }

      val userId = elements.last.userId.toString
      val offset = bloom.hashCode(userId,61)
      val isEexit = jedist.getbit(StorKey,offset)
      if(!isEexit){
       jedist.setbit(StorKey,offset,true)
        jedist.hset("count",StorKey,(count+1).toString)
       out.collect(ItemViewCount(StorKey.toLong, context.window.getEnd,count+1))
      }else
        {
          out.collect(ItemViewCount(StorKey.toLong, context.window.getEnd,count))
        }
    }
  }

  class Bloom(size:Long) extends Serializable {
    private val cep = if(size >0) size else 1<<27

    def  hashCode(value:String,seed:Int): Long ={
      var result = 0L
      for (i<-0 until value.length){
        result=result*seed+value.charAt(i)
      }
      result & (cep-1)

    }
  }

  // 自定义实现聚合函数

  class CountAgg extends AggregateFunction[UserBehavior, Long, Long]{
    override def add(value: UserBehavior, accumulator: Long) = accumulator+1

    override def createAccumulator(): Long = 0L

    override def getResult(accumulator:Long): Long = accumulator

    override def merge(a: Long, b: Long): Long = a+b
  }

  // 自定义实现Window Function，输出ItemViewCount格式
  class WindowResultFunction extends WindowFunction[Long, ItemViewCount, Tuple, TimeWindow]{
    override def apply(key: Tuple, window: TimeWindow, input: Iterable[Long], out: Collector[ItemViewCount]): Unit = {
      val itemId = key.asInstanceOf[Tuple1[Long]].f0
      val count = input.iterator.next()
      import scala.collection.JavaConverters._
      out.collect(ItemViewCount(itemId, window.getEnd, count))
    }
  }
  class BollomProcess extends ProcessWindowFunction[UserBehavior,ItemViewCount,Tuple,TimeWindow]{

   // var wordState: MapState[String, String] = _
   val roaring64NavigableDesc = new ValueStateDescriptor[Roaring64NavigableMap]("roaring64NavigableDesc", classOf[Roaring64NavigableMap])
    var roaringTest: ValueState[Roaring64NavigableMap] = _

    override def process(key: Tuple, context: Context, elements: Iterable[UserBehavior], out: Collector[ItemViewCount]): Unit = {
      val itemId = key.asInstanceOf[Tuple1[Long]].f0
      val elementList = elements.iterator.toList
     val roaring64Navigable= context.windowState.getState(roaring64NavigableDesc).value()
       var elementSet = Set[UserBehavior]()
//      var roaring64Navigable = new Roaring64NavigableMap
      val sdf = new SimpleDateFormat("yyyyMMdd")
      val str = sdf.format(System.currentTimeMillis())
      for(userBehaive <-elementList){
          val bigLong =userBehaive.userId.toString+str.toString
        roaring64Navigable.addLong(bigLong.toLong)
       // elementSet+=userBehaive
      }
     val count= roaring64Navigable.getLongCardinality
//      roaring64Navigable.runOptimize()
   //   if(roaring64Navigable!=null){
    //     count =roaring.getLongCardinality
    //    val ff = roaring.getIntCardinality
      //  pvCount.update(roaring64Navigable)
   //   }

    //  println("count==="+count)
//      if(elementList.size>2){
//        println("coutnt==="+count)
//        println("elemList====="+elementList.size)
//      }

      out.collect(ItemViewCount(itemId, context.window.getEnd, count))
    }

    override def open(parameters: Configuration): Unit = {
    super.open(parameters)
      //      // 命名状态变量的名字和类型
      //      val itemStateDesc = new ListStateDescriptor[ItemViewCount]("itemState", classOf[ItemViewCount])
      //      itemState = getRuntimeContext.getListState(itemStateDesc)
    //  wordState = getRuntimeContext.getMapState(new MapStateDescriptor[String, String]("word", classOf[String], classOf[String]))

  //    val roaring64NavigableMap = new Roaring64NavigableMap

      roaringTest = getRuntimeContext.getState(roaring64NavigableDesc)
    }
  }

  // 自定义实现process function
  class TopNHotItems(topSize: Int) extends KeyedProcessFunction[Tuple, ItemViewCount, String]{

    // 定义状态ListState
    private var itemState: ListState[ItemViewCount] = _

    override def open(parameters: Configuration): Unit = {
      super.open(parameters)
      // 命名状态变量的名字和类型
      val itemStateDesc = new ListStateDescriptor[ItemViewCount]("itemState", classOf[ItemViewCount])
      itemState = getRuntimeContext.getListState(itemStateDesc)
    }

    override def processElement(i: ItemViewCount, context: KeyedProcessFunction[Tuple, ItemViewCount, String]#Context, collector: Collector[String]): Unit = {
      itemState.add(i)
      // 注册定时器，触发时间定为 windowEnd + 1，触发时说明window已经收集完成所有数据
      context.timerService.registerEventTimeTimer( i.windowEnd + 1 )
    }

    // 定时器触发操作，从state里取出所有数据，排序取TopN，输出
    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Tuple, ItemViewCount, String]#OnTimerContext, out: Collector[String]): Unit = {
      // 获取所有的商品点击信息
      val allItems: ListBuffer[ItemViewCount] = ListBuffer()
      import  scala.collection.JavaConversions._
      for(item <- itemState.get){
        allItems += item
      }
      // 清除状态中的数据，释放空间
      itemState.clear()

      // 按照点击量从大到小排序，选取TopN
      val sortedItems = allItems.sortBy(_.count)(Ordering.Long.reverse).take(topSize)

      // 将排名数据格式化，便于打印输出
      val result: StringBuilder = new StringBuilder
      result.append("====================================\n")
      result.append("时间：").append(new Timestamp(timestamp - 1)).append("\n")

      for( i <- sortedItems.indices ){
        val currentItem: ItemViewCount = sortedItems(i)
        // 输出打印的格式 e.g.  No1：  商品ID=12224  浏览量=2413
        result.append("No").append(i+1).append(":")
          .append("  商品ID=").append(currentItem.itemId)
          .append("  浏览量=").append(currentItem.count).append("\n")
      }
      result.append("====================================\n\n")
      // 控制输出频率
      Thread.sleep(100)
      out.collect(result.toString)
    }
  }
}
