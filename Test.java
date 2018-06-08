package com.szu.esu.cn;

import java.io.IOException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.hc.client5.http.impl.sync.CloseableHttpClient;
import org.apache.hc.client5.http.impl.sync.HttpClients;
import org.apache.hc.client5.http.methods.HttpGet;
import org.apache.hc.client5.http.protocol.ClientProtocolException;
import org.apache.hc.client5.http.sync.HttpClient;
import org.apache.hc.client5.http.sync.ResponseHandler;
import org.apache.hc.core5.http.HttpEntity;
import org.apache.hc.core5.http.HttpResponse;
import org.apache.hc.core5.http.ParseException;
import org.apache.hc.core5.http.entity.EntityUtils;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.types.BSONTimestamp;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.parser.Feature;
import com.cninfo.mongodb2esexportsystem.bean.Record;
import com.cninfo.mongodb2esexportsystem.db.BulkESDataSource;
import com.cninfo.mongodb2esexportsystem.db.DataSource;
import com.cninfo.mongodb2esexportsystem.db.MongodbDataSource;
import com.cninfo.mongodb2esexportsystem.db.OperationResult;
import com.google.gson.Gson;
import com.google.gson.JsonNull;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.internal.LinkedTreeMap;

class Test {
	private MongodbDataSource srcDataSource=null;
	private CloseableHttpClient httpClient;
  	String srcMongoDatabaseName = "es_export_test"; // all_type_data/DevelopmentTest
  	String authMongoDatabaseName = "es_export_test";	// all_type_data/DevelopmentTest
	
	//MongoDB的ip+host地址
	private String mongoDBIp="172.26.18.109:27017";
	
	private int time=0;
	
	//MongoInsert的Record
	private List<Record> mongoRecord;
	//MongoInsert记录的ids
	private Map ids;
	
	//EsUpdate的Record
	private Map esRecord=new HashMap();
	
	//ES的地址
	private String esIp="http://172.26.18.108:9200/";
	
	//保存Mongo更新对应的id和属性值
	//用于保存id和更新的属性值
	Map saveMongoUpdateMap=null;
	
	
	//用于保存更新的Mongo记录的条件ConditionUpdate(主表的更新条件，注：对于更新关联表不适用)
	Map conditionUpdate=null;
	

	//用于保存更新的Custoemr和Supplier记录的条件ConditionUpdate(主表的更新条件)
	Map conditionCusAndSupUpdate=null;
	
	@BeforeEach
	void setup()
	{
		//创建MongoUpdateMap
		saveMongoUpdateMap=new HashMap();
	    String upFieldName = "_id";
	  	int exporterThreadNum = 20;
	  	int loaderThreadNum = 1;
	  	int converterThreadNum = 10;
	  	int saverThreadNum = 1; // 只能是一个
	  	srcDataSource = new MongodbDataSource("MongoDatabase", 
	        		Arrays.asList(mongoDBIp), "es_test_user", "es_test_user", authMongoDatabaseName, 
	        		srcMongoDatabaseName, null); // pmreaduser pmreaduser1957 pmreaduser2/pmreaduser2x
	  	System.out.println("创建了MongodbDataSource");
	  	httpClient=HttpClients.createDefault();
	  	//System.out.println(httpClient.toString());
	  	initUpdateConditional();
	  	initUpdateCusAndSupConditional();
	}
	
	public void initUpdateConditional()
	{
		conditionUpdate=new HashMap();
		conditionUpdate.put("opType", "query");
  	    Map data=new HashMap();
  	    conditionUpdate.put("data", data);
  	    Map filterMap = new HashMap();
  	    Map ltMap  = new HashMap();
  	    ltMap.put("$gt", new ObjectId("000000000000000000000000"));
  	    filterMap.put("_id", ltMap);
  	    data.put("filter", filterMap);
  	    data.put("limit", 1000);
	}
	
	public void initUpdateCusAndSupConditional()
	{
		conditionCusAndSupUpdate=new HashMap();
		conditionCusAndSupUpdate.put("opType", "query");
  	    Map data=new HashMap();
  	    conditionCusAndSupUpdate.put("data", data);
  	    Map filterMap = new HashMap();
  	    Map eqMap  = new HashMap();
  	    eqMap.put("$eq", "2017");
  	    filterMap.put("update_time",eqMap);
  	    data.put("filter", filterMap);
  	    data.put("limit", 1000);
	}
	
	//测试索引Patent
	@org.junit.jupiter.api.Test
	void testPatent() throws IOException, InterruptedException {
	  
  	  String collectionName="sanban_company_patent";
  	  Map conditonal=new HashMap();
  	  conditonal.put("opType", "query");
  	  Map data=new HashMap();
  	  conditonal.put("data", data);
  	  Map filterMap = new HashMap();
  	  Map gtMap  = new HashMap();
  	  gtMap.put("$gt", new ObjectId("000000000000000000001000"));
  	  filterMap.put("_id", gtMap);
  	  Map projectionMap=new HashMap();
  	  projectionMap.put("_id", 0);
  	  data.put("filter", filterMap);
  	  data.put("limit", 10);
  	  data.put("projection", projectionMap);
  	  //查询数据并新增数据
  	  insertToMongo(collectionName, conditonal);
  	  Thread.sleep(10000);
  	  getESUpdate(ids, "patent_test");
  	  Map compareMap=new HashMap();
  	  //设置需要比对的字段
  	  compareMap.put(0,"company_ids;companyId");
  	  compareMap.put(1,"createtime;createTime");
  	  compareMap.put(2,"title;patentName");
  	  compareMap.put(3,"createnum;patentNumber");
  	  compareMap.put(4,"patentorg;patentOwnOrg");
  	  compareMap.put(5,"patenter;patentOwner");
  	  compareMap.put(6,"ipctype;ipcType");
  	  compareMap.put(7,"summary;patentSummary");
  	  if(compareMongoAndEs(compareMap))
  	  {
  		  System.out.println("同步更新成功,测试完成，程序正常");
  	  };
  	  
	}
	
	//测试patent的update字段company_id
	@org.junit.jupiter.api.Test
	void testPatentUpdateCompanyId() throws Exception
	{
	  //测试的mongo表名	
	  String collectionName="sanban_company_patent";
	  Map ids=new HashMap();
	  //需要更新的key和value
	  Map newValue=new HashMap();
	  newValue.put("company_id", 1000);
	  //拿到更新后的数据
	  updateToMongo(collectionName, conditionUpdate, newValue);
	  //拿到更新后的数据
	  mongoRecord=srcDataSource.query(collectionName, conditionUpdate);
	  for(int i=0;i<mongoRecord.size();i++)
	  {
		  ids.put(i+1, mongoRecord.get(i).getData().get("_id"));
	  }
	  Thread.sleep(5000);
	  getESUpdate(ids, "patent_test");
	  Map compareMap=new HashMap();
	  //设置需要比对的字段
	  compareMap.put(0,"company_id;companyId");
	  if(compareMongoAndEs(compareMap)==true)
	  {
		  System.out.println("同步更新成功");
	  }
	  else {
		  System.out.println("同步更新失败");
	  }	
	}		
		
	//测试patent的update字段title
	@org.junit.jupiter.api.Test
	void testPatentUpdateTitle() throws Exception
	{
	  //测试的mongo表名	
	  String collectionName="sanban_company_patent";
	  Map ids=new HashMap();
	  //需要更新的key和value
	  Map newValue=new HashMap();
	  newValue.put("title", "修改后是一种用风冷结晶的节能设备");
	  //拿到更新后的数据
	  updateToMongo(collectionName, conditionUpdate, newValue);
	  //拿到更新后的数据
	  mongoRecord=srcDataSource.query(collectionName, conditionUpdate);
	  for(int i=0;i<mongoRecord.size();i++)
	  {
		  ids.put(i+1, mongoRecord.get(i).getData().get("_id"));
	  }
	  Thread.sleep(5000);
	  getESUpdate(ids, "patent_test");
	  Map compareMap=new HashMap();
	  //设置需要比对的字段
	  compareMap.put(0,"title;patentName");
	  if(compareMongoAndEs(compareMap)==true)
	  {
		  System.out.println("同步更新成功");
	  }
	  else {
		  System.out.println("同步更新失败");
	  }	
	}		
	
	//测试patent的update字段patentOrg
	@org.junit.jupiter.api.Test
	void testPatentUpdatePatentOrg() throws Exception
	{
	  //测试的mongo表名	
	  String collectionName="sanban_company_patent";
	  Map ids=new HashMap();
	  //需要更新的key和value
	  Map newValue=new HashMap();
	  newValue.put("patentorg", "修改后昆山三景科技股份有限公司");
	  //拿到更新后的数据
	  updateToMongo(collectionName, conditionUpdate, newValue);
	  //拿到更新后的数据
	  mongoRecord=srcDataSource.query(collectionName, conditionUpdate);
	  for(int i=0;i<mongoRecord.size();i++)
	  {
		  ids.put(i+1, mongoRecord.get(i).getData().get("_id"));
	  }
	  Thread.sleep(5000);
	  getESUpdate(ids, "patent_test");
	  Map compareMap=new HashMap();
	  //设置需要比对的字段
	  compareMap.put(0,"patentorg;patentOwnOrg");
	  if(compareMongoAndEs(compareMap)==true)
	  {
		  System.out.println("同步更新成功");
	  }
	  else {
		  System.out.println("同步更新失败");
	  }	
	}		
	
	//测试patent的update字段patenter
	@org.junit.jupiter.api.Test
	void testPatentUpdatePatenter() throws Exception
	{
	  //测试的mongo表名	
	  String collectionName="sanban_company_patent";
	  Map ids=new HashMap();
	  //需要更新的key和value
	  Map newValue=new HashMap();
	  newValue.put("patenter", "蔡一青");
	  //拿到更新后的数据
	  updateToMongo(collectionName, conditionUpdate, newValue);
	  //拿到更新后的数据
	  mongoRecord=srcDataSource.query(collectionName, conditionUpdate);
	  for(int i=0;i<mongoRecord.size();i++)
	  {
		  ids.put(i+1, mongoRecord.get(i).getData().get("_id"));
	  }
	  Thread.sleep(5000);
	  getESUpdate(ids, "patent_test");
	  Map compareMap=new HashMap();
	  //设置需要比对的字段
	  compareMap.put(0,"patenter;patentOwner");
	  if(compareMongoAndEs(compareMap)==true)
	  {
		  System.out.println("同步更新成功");
	  }
	  else {
		  System.out.println("同步更新失败");
	  }	
	}		

	//测试patent的update字段ipctype
	@org.junit.jupiter.api.Test
	void testPatentUpdateIpctype() throws Exception
	{
	  //测试的mongo表名	
	  String collectionName="sanban_company_patent";
	  Map ids=new HashMap();
	  //需要更新的key和value
	  Map newValue=new HashMap();
	  newValue.put("ipctype", "SCPD2013");
	  //拿到更新后的数据
	  updateToMongo(collectionName, conditionUpdate, newValue);
	  //拿到更新后的数据
	  mongoRecord=srcDataSource.query(collectionName, conditionUpdate);
	  for(int i=0;i<mongoRecord.size();i++)
	  {
		  ids.put(i+1, mongoRecord.get(i).getData().get("_id"));
	  }
	  Thread.sleep(5000);
	  getESUpdate(ids, "patent_test");
	  Map compareMap=new HashMap();
	  //设置需要比对的字段
	  compareMap.put(0,"ipctype;ipcType");
	  if(compareMongoAndEs(compareMap)==true)
	  {
		  System.out.println("同步更新成功");
	  }
	  else {
		  System.out.println("同步更新失败");
	  }	
	}		
	
	//测试patent的update字段createnum
	@org.junit.jupiter.api.Test
	void testPatentUpdateCreatenum() throws Exception
	{
	  //测试的mongo表名	
	  String collectionName="sanban_company_patent";
	  Map ids=new HashMap();
	  //需要更新的key和value
	  Map newValue=new HashMap();
	  newValue.put("createnum", "CN204613217U");
	  //拿到更新后的数据
	  updateToMongo(collectionName, conditionUpdate, newValue);
	  //拿到更新后的数据
	  mongoRecord=srcDataSource.query(collectionName, conditionUpdate);
	  for(int i=0;i<mongoRecord.size();i++)
	  {
		  ids.put(i+1, mongoRecord.get(i).getData().get("_id"));
	  }
	  Thread.sleep(5000);
	  getESUpdate(ids, "patent_test");
	  Map compareMap=new HashMap();
	  //设置需要比对的字段
	  compareMap.put(0,"createnum;patentNumber");
	  if(compareMongoAndEs(compareMap)==true)
	  {
		  System.out.println("同步更新成功");
	  }
	  else {
		  System.out.println("同步更新失败");
	  }	
	}		
	
	//测试索引Websites
	@org.junit.jupiter.api.Test
	void testWebsites() throws IOException, InterruptedException {
	  
  	  String collectionName="sanban_company_web";
  	  Map conditonal=new HashMap();
  	  conditonal.put("opType", "query");
  	  Map data=new HashMap();
  	  conditonal.put("data", data);
  	  Map filterMap = new HashMap();
  	  Map gtMap  = new HashMap();
  	  gtMap.put("$gt", new ObjectId("000000000000000000000500"));
  	  filterMap.put("_id", gtMap);
  	  Map projectionMap=new HashMap();
  	  projectionMap.put("_id", 0);
  	  data.put("filter", filterMap);
  	  data.put("limit", 500);
  	  data.put("projection", projectionMap);
  	  //查询数据并新增数据
  	  insertToMongo(collectionName, conditonal);
  	  Thread.sleep(10000);
  	  getESUpdate(ids, "websites_test");
  	  Map compareMap=new HashMap();
  	  //设置需要比对的字段
  	  compareMap.put(0,"company_id;companyId");
  	  compareMap.put(1,"create_date;createDate");
  	  compareMap.put(2,"name;name");
  	  compareMap.put(3,"url;url");
  	  compareMap.put(4,"num;recordNumber");
  	  compareMap.put(5,"status;status");
  	  compareMap.put(6,"web_type;applicantType");
  	  if(compareMongoAndEs(compareMap))
  	  {
  		  System.out.println("同步更新成功,测试完成，程序正常");
  	  };
  	  
	}
	
	//更新websites表的company_id
	@org.junit.jupiter.api.Test
	void testWebsitesUpdateCompanyId() throws Exception
	{
	  //测试的mongo表名	
	  String collectionName="sanban_company_web";
	  Map ids=new HashMap();
	  //需要更新的key和value
	  Map newValue=new HashMap();
	  newValue.put("company_id", 100);
	  //拿到更新后的数据
	  updateToMongo(collectionName, conditionUpdate, newValue);
	  //拿到更新后的数据
	  mongoRecord=srcDataSource.query(collectionName, conditionUpdate);
	  for(int i=0;i<mongoRecord.size();i++)
	  {
		  ids.put(i+1, mongoRecord.get(i).getData().get("_id"));
	  }
	  Thread.sleep(5000);
	  getESUpdate(ids, "websites_test");
	  Map compareMap=new HashMap();
	  //设置需要比对的字段
	  compareMap.put(0,"company_id;companyId");
	  if(compareMongoAndEs(compareMap)==true)
	  {
		  System.out.println("同步更新成功");
	  }
	  else {
		  System.out.println("同步更新失败");
	  }	
	}		
	
	//更新websites表的create_date
	@org.junit.jupiter.api.Test
	void testWebsitesUpdateCreateDate() throws Exception
	{
	  //测试的mongo表名	
	  String collectionName="sanban_company_web";
	  Map ids=new HashMap();
	  //需要更新的key和value
	  Map newValue=new HashMap();
	  newValue.put("create_date", "2018-06-01");
	  //拿到更新后的数据
	  updateToMongo(collectionName, conditionUpdate, newValue);
	  //拿到更新后的数据
	  mongoRecord=srcDataSource.query(collectionName, conditionUpdate);
	  for(int i=0;i<mongoRecord.size();i++)
	  {
		  ids.put(i+1, mongoRecord.get(i).getData().get("_id"));
	  }
	  Thread.sleep(5000);
	  getESUpdate(ids, "websites_test");
	  Map compareMap=new HashMap();
	  //设置需要比对的字段
	  compareMap.put(0,"create_date;createDate");
	  if(compareMongoAndEs(compareMap)==true)
	  {
		  System.out.println("同步更新成功");
	  }
	  else {
		  System.out.println("同步更新失败");
	  }		
	}
	
	//更新websites表的name
	@org.junit.jupiter.api.Test
	void testWebsitesUpdateName() throws Exception
	{
	  //测试的mongo表名	
	  String collectionName="sanban_company_web";
	  Map ids=new HashMap();
	  //需要更新的key和value
	  Map newValue=new HashMap();
	  newValue.put("name", "update万科公益基金会");
	  //拿到更新后的数据
	  updateToMongo(collectionName, conditionUpdate, newValue);
	  //拿到更新后的数据
	  mongoRecord=srcDataSource.query(collectionName, conditionUpdate);
	  for(int i=0;i<mongoRecord.size();i++)
	  {
		  ids.put(i+1, mongoRecord.get(i).getData().get("_id"));
	  }
	  Thread.sleep(5000);
	  getESUpdate(ids, "websites_test");
	  Map compareMap=new HashMap();
	  //设置需要比对的字段
	  compareMap.put(0,"name;name");
	  if(compareMongoAndEs(compareMap)==true)
	  {
		  System.out.println("同步更新成功");
	  }
	  else {
		  System.out.println("同步更新失败");
	  }		
	}
	
	//更新websites表的url
	@org.junit.jupiter.api.Test
	void testWebsitesUpdateUrl() throws Exception
	{
	  //测试的mongo表名	
	  String collectionName="sanban_company_web";
	  Map ids=new HashMap();
	  //需要更新的key和value
	  Map newValue=new HashMap();
	  newValue.put("url", "www.vankefoundation.org");
	  //拿到更新后的数据
	  updateToMongo(collectionName, conditionUpdate, newValue);
	  //拿到更新后的数据
	  mongoRecord=srcDataSource.query(collectionName, conditionUpdate);
	  for(int i=0;i<mongoRecord.size();i++)
	  {
		  ids.put(i+1, mongoRecord.get(i).getData().get("_id"));
	  }
	  Thread.sleep(5000);
	  getESUpdate(ids, "websites_test");
	  Map compareMap=new HashMap();
	  //设置需要比对的字段
	  compareMap.put(0,"url;url");
	  if(compareMongoAndEs(compareMap)==true)
	  {
		  System.out.println("同步更新成功");
	  }
	  else {
		  System.out.println("同步更新失败");
	  }		
	}
	
	//更新websites表的num
	@org.junit.jupiter.api.Test
	void testWebsitesUpdateNum() throws Exception
	{
	  //测试的mongo表名	
	  String collectionName="sanban_company_web";
	  Map ids=new HashMap();
	  //需要更新的key和value
	  Map newValue=new HashMap();
	  newValue.put("num", "粤ICP备05098314号-3");
	  //拿到更新后的数据
	  updateToMongo(collectionName, conditionUpdate, newValue);
	  //拿到更新后的数据
	  mongoRecord=srcDataSource.query(collectionName, conditionUpdate);
	  for(int i=0;i<mongoRecord.size();i++)
	  {
		  ids.put(i+1, mongoRecord.get(i).getData().get("_id"));
	  }
	  Thread.sleep(5000);
	  getESUpdate(ids, "websites_test");
	  Map compareMap=new HashMap();
	  //设置需要比对的字段
	  compareMap.put(0,"num;recordNumber");
	  if(compareMongoAndEs(compareMap)==true)
	  {
		  System.out.println("同步更新成功");
	  }
	  else {
		  System.out.println("同步更新失败");
	  }		
	}
	
	//更新websites表的status
	@org.junit.jupiter.api.Test
	void testWebsitesUpdateStatus() throws Exception
	{
	  //测试的mongo表名	
	  String collectionName="sanban_company_web";
	  Map ids=new HashMap();
	  //需要更新的key和value
	  Map newValue=new HashMap();
	  newValue.put("status", "update正常");
	  //拿到更新后的数据
	  updateToMongo(collectionName, conditionUpdate, newValue);
	  //拿到更新后的数据
	  mongoRecord=srcDataSource.query(collectionName, conditionUpdate);
	  for(int i=0;i<mongoRecord.size();i++)
	  {
		  ids.put(i+1, mongoRecord.get(i).getData().get("_id"));
	  }
	  Thread.sleep(5000);
	  getESUpdate(ids, "websites_test");
	  Map compareMap=new HashMap();
	  //设置需要比对的字段
	  compareMap.put(0,"status;status");
	  if(compareMongoAndEs(compareMap)==true)
	  {
		  System.out.println("同步更新成功");
	  }
	  else {
		  System.out.println("同步更新失败");
	  }		
	}

	//更新websites表的web_type
	@org.junit.jupiter.api.Test
	void testWebsitesUpdateWebType() throws Exception
	{
	  //测试的mongo表名	
	  String collectionName="sanban_company_web";
	  Map ids=new HashMap();
	  //需要更新的key和value
	  Map newValue=new HashMap();
	  newValue.put("web_type", "update企业");
	  //拿到更新后的数据
	  updateToMongo(collectionName, conditionUpdate, newValue);
	  //拿到更新后的数据
	  mongoRecord=srcDataSource.query(collectionName, conditionUpdate);
	  for(int i=0;i<mongoRecord.size();i++)
	  {
		  ids.put(i+1, mongoRecord.get(i).getData().get("_id"));
	  }
	  Thread.sleep(5000);
	  getESUpdate(ids, "websites_test");
	  Map compareMap=new HashMap();
	  //设置需要比对的字段
	  compareMap.put(0,"web_type;applicantType");
	  if(compareMongoAndEs(compareMap)==true)
	  {
		  System.out.println("同步更新成功");
	  }
	  else {
		  System.out.println("同步更新失败");
	  }		
	}
		
	//测试索引Supplier
	@org.junit.jupiter.api.Test
	void testSupplier() throws IOException, InterruptedException {  
  	  String collectionName="sanban_company_supplier";
  	  Map conditonal=new HashMap();
  	  conditonal.put("opType", "query");
  	  Map data=new HashMap();
  	  conditonal.put("data", data);
  	  Map filterMap = new HashMap();
  	  Map eqMap  = new HashMap();
  	  eqMap.put("$eq", "2017");
  	  filterMap.put("update_time", eqMap);
  	  Map projectionMap=new HashMap();
  	  projectionMap.put("_id", 0);
  	  data.put("filter", filterMap);
  	  data.put("limit", 500);
  	  data.put("projection", projectionMap);
  	  //查询数据并新增数据
  	  insertToMongo(collectionName, conditonal);
  	  Thread.sleep(30000);
  	  getESUpdate(ids, "supplier_test");
  	  Map compareMap=new HashMap();
  	  //设置需要比对的字段
  	  compareMap.put(0,"company_id;companyId");
  	  compareMap.put(1,"supplier_name;supplierName");
  	  compareMap.put(2,"realation_id;relationId");
  	  compareMap.put(3,"market_num;marketNum");
  	  compareMap.put(4,"percent;percent");
  	  compareMap.put(5,"content;content");
  	  if(compareMongoAndEs(compareMap))
  	  {
  		  System.out.println("同步更新成功,测试完成，程序正常");
  	  };
  	  
	}

	//更新Supplier表的company_id
	@org.junit.jupiter.api.Test
	void tesSupplierUpdateCompanyId() throws Exception
	{
	  //测试的mongo表名	
	  String collectionName="sanban_company_supplier";
	  Map ids=new HashMap();
	  //需要更新的key和value
	  Map newValue=new HashMap();
	  newValue.put("company_id", 100);
	  //customer的更新条件需要调整
	  //拿到更新后的数据
	  updateToMongo(collectionName, conditionCusAndSupUpdate, newValue);
	  //拿到更新后的数据
	  mongoRecord=srcDataSource.query(collectionName, conditionCusAndSupUpdate);
	  for(int i=0;i<mongoRecord.size();i++)
	  {
		  ids.put(i+1, mongoRecord.get(i).getData().get("_id"));
	  }
	  Thread.sleep(5000);
	  getESUpdate(ids, "supplier_test");
	  Map compareMap=new HashMap();
	  //设置需要比对的字段
	  compareMap.put(0,"company_id;companyId");
	  if(compareMongoAndEs(compareMap)==true)
	  {
		  System.out.println("同步更新成功");
	  }
	  else {
		  System.out.println("同步更新失败");
	  }	
	}		
	
	//更新Supplier表的supplier_name
	@org.junit.jupiter.api.Test
	void tesSupplierUpdateSupplierName() throws Exception
	{
	  //测试的mongo表名	
	  String collectionName="sanban_company_supplier";
	  Map ids=new HashMap();
	  //需要更新的key和value
	  Map newValue=new HashMap();
	  newValue.put("supplier_name", "广州市益都贸易有限公司");
	  //customer的更新条件需要调整
	  //拿到更新后的数据
	  updateToMongo(collectionName, conditionCusAndSupUpdate, newValue);
	  //拿到更新后的数据
	  mongoRecord=srcDataSource.query(collectionName, conditionCusAndSupUpdate);
	  for(int i=0;i<mongoRecord.size();i++)
	  {
		  ids.put(i+1, mongoRecord.get(i).getData().get("_id"));
	  }
	  Thread.sleep(5000);
	  getESUpdate(ids, "supplier_test");
	  Map compareMap=new HashMap();
	  //设置需要比对的字段
	  compareMap.put(0,"supplier_name;supplierName");
	  if(compareMongoAndEs(compareMap)==true)
	  {
		  System.out.println("同步更新成功");
	  }
	  else {
		  System.out.println("同步更新失败");
	  }	
	}		
	
	//更新Supplier表的realation_id
	@org.junit.jupiter.api.Test
	void tesSupplierUpdateRealationId() throws Exception
	{
	  //测试的mongo表名	
	  String collectionName="sanban_company_supplier";
	  Map ids=new HashMap();
	  //需要更新的key和value
	  Map newValue=new HashMap();
	  newValue.put("realation_id", 95675);
	  //customer的更新条件需要调整
	  //拿到更新后的数据
	  updateToMongo(collectionName, conditionCusAndSupUpdate, newValue);
	  //拿到更新后的数据
	  mongoRecord=srcDataSource.query(collectionName, conditionCusAndSupUpdate);
	  for(int i=0;i<mongoRecord.size();i++)
	  {
		  ids.put(i+1, mongoRecord.get(i).getData().get("_id"));
	  }
	  Thread.sleep(5000);
	  getESUpdate(ids, "supplier_test");
	  Map compareMap=new HashMap();
	  //设置需要比对的字段
	  compareMap.put(0,"realation_id;realationId");
	  if(compareMongoAndEs(compareMap)==true)
	  {
		  System.out.println("同步更新成功");
	  }
	  else {
		  System.out.println("同步更新失败");
	  }	
	}

	//更新Supplier表的market_num
	@org.junit.jupiter.api.Test
	void tesSupplierUpdateMarketNum() throws Exception
	{
	  //测试的mongo表名	
	  String collectionName="sanban_company_supplier";
	  Map ids=new HashMap();
	  //需要更新的key和value
	  Map newValue=new HashMap();
	  newValue.put("market_num", 95675.88);
	  //customer的更新条件需要调整
	  //拿到更新后的数据
	  updateToMongo(collectionName, conditionCusAndSupUpdate, newValue);
	  //拿到更新后的数据
	  mongoRecord=srcDataSource.query(collectionName, conditionCusAndSupUpdate);
	  for(int i=0;i<mongoRecord.size();i++)
	  {
		  ids.put(i+1, mongoRecord.get(i).getData().get("_id"));
	  }
	  Thread.sleep(5000);
	  getESUpdate(ids, "supplier_test");
	  Map compareMap=new HashMap();
	  //设置需要比对的字段
	  compareMap.put(0,"market_num;marketNum");
	  if(compareMongoAndEs(compareMap)==true)
	  {
		  System.out.println("同步更新成功");
	  }
	  else {
		  System.out.println("同步更新失败");
	  }	
	}
	
	//更新Supplier表的percent
	@org.junit.jupiter.api.Test
	void tesSupplierUpdatePercent() throws Exception
	{
	  //测试的mongo表名	
	  String collectionName="sanban_company_supplier";
	  Map ids=new HashMap();
	  //需要更新的key和value
	  Map newValue=new HashMap();
	  newValue.put("percent", "2.88");
	  //customer的更新条件需要调整
	  //拿到更新后的数据
	  updateToMongo(collectionName, conditionCusAndSupUpdate, newValue);
	  //拿到更新后的数据
	  mongoRecord=srcDataSource.query(collectionName, conditionCusAndSupUpdate);
	  for(int i=0;i<mongoRecord.size();i++)
	  {
		  ids.put(i+1, mongoRecord.get(i).getData().get("_id"));
	  }
	  Thread.sleep(5000);
	  getESUpdate(ids, "supplier_test");
	  Map compareMap=new HashMap();
	  //设置需要比对的字段
	  compareMap.put(0,"percent;percent");
	  if(compareMongoAndEs(compareMap)==true)
	  {
		  System.out.println("同步更新成功");
	  }
	  else {
		  System.out.println("同步更新失败");
	  }	
	}
	
	//更新Supplier表的content
	@org.junit.jupiter.api.Test
	void tesSupplierUpdateContent() throws Exception
	{
	  //测试的mongo表名	
	  String collectionName="sanban_company_supplier";
	  Map ids=new HashMap();
	  //需要更新的key和value
	  Map newValue=new HashMap();
	  newValue.put("content", "江南皮革厂倒闭了！！");
	  //customer的更新条件需要调整
	  //拿到更新后的数据
	  updateToMongo(collectionName, conditionCusAndSupUpdate, newValue);
	  //拿到更新后的数据
	  mongoRecord=srcDataSource.query(collectionName, conditionCusAndSupUpdate);
	  for(int i=0;i<mongoRecord.size();i++)
	  {
		  ids.put(i+1, mongoRecord.get(i).getData().get("_id"));
	  }
	  Thread.sleep(5000);
	  getESUpdate(ids, "supplier_test");
	  Map compareMap=new HashMap();
	  //设置需要比对的字段
	  compareMap.put(0,"content;content");
	  if(compareMongoAndEs(compareMap)==true)
	  {
		  System.out.println("同步更新成功");
	  }
	  else {
		  System.out.println("同步更新失败");
	  }	
	}
	
	//测试索引Customer
	@org.junit.jupiter.api.Test
	void testCoustomer() throws IOException, InterruptedException {  
  	  String collectionName="sanban_company_customer";
  	  Map conditonal=new HashMap();
  	  conditonal.put("opType", "query");
  	  Map data=new HashMap();
  	  conditonal.put("data", data);
  	  Map filterMap = new HashMap();
  	  Map gtMap  = new HashMap();
  	  gtMap.put("$eq", "2017");
  	  filterMap.put("update_time", gtMap);
  	  Map projectionMap=new HashMap();
  	  projectionMap.put("_id", 0);
  	  data.put("filter", filterMap);
  	  data.put("limit", 10);
  	  data.put("projection", projectionMap);
  	  //查询数据并新增数据
  	  insertToMongo(collectionName, conditonal);
  	  
  	  Thread.sleep(10000);
  	  getESUpdate(ids, "customers_test");
  	  Map compareMap=new HashMap();
  	  //设置需要比对的字段
  	  compareMap.put(0,"company_id;companyId");
  	  compareMap.put(1,"customer_name;customerName");
  	  compareMap.put(2,"realation_id;relationId");
  	  compareMap.put(3,"market_num;marketNum");
  	  compareMap.put(4,"percent;percent");
  	  compareMap.put(5,"content;content");
  	  if(compareMongoAndEs(compareMap))
  	  {
  		  System.out.println("同步更新成功,测试完成，程序正常");
  	  };
  	  
	}

	//更新Customer表的company_id
	@org.junit.jupiter.api.Test
	void testCustomerUpdateCompanyId() throws Exception
	{
	  //测试的mongo表名	
	  String collectionName="sanban_company_customer";
	  Map ids=new HashMap();
	  //需要更新的key和value
	  Map newValue=new HashMap();
	  newValue.put("company_id", 999999);
	  //customer的更新条件需要调整
	  //拿到更新后的数据
	  updateToMongo(collectionName, conditionCusAndSupUpdate, newValue);
	  //拿到更新后的数据
	  mongoRecord=srcDataSource.query(collectionName, conditionCusAndSupUpdate);
	  for(int i=0;i<mongoRecord.size();i++)
	  {
		  ids.put(i+1, mongoRecord.get(i).getData().get("_id"));
	  }
	  Thread.sleep(5000);
	  getESUpdate(ids, "customers_test");
	  Map compareMap=new HashMap();
	  //设置需要比对的字段
	  compareMap.put(0,"company_id;companyId");
	  if(compareMongoAndEs(compareMap)==true)
	  {
		  System.out.println("同步更新成功");
	  }
	  else {
		  System.out.println("同步更新失败");
	  }	
	}		
	
	//更新Customer表的customer_name
	@org.junit.jupiter.api.Test
	void testCustomerUpdateCustomerName() throws Exception
	{
	  //测试的mongo表名	
	  String collectionName="sanban_company_customer";
	  Map ids=new HashMap();
	  //需要更新的key和value
	  Map newValue=new HashMap();
	  newValue.put("customer_name", "小蔡");
	  //customer的更新条件需要调整
	  //拿到更新后的数据
	  updateToMongo(collectionName, conditionCusAndSupUpdate, newValue);
	  //拿到更新后的数据
	  mongoRecord=srcDataSource.query(collectionName, conditionCusAndSupUpdate);
	  for(int i=0;i<mongoRecord.size();i++)
	  {
		  ids.put(i+1, mongoRecord.get(i).getData().get("_id"));
	  }
	  Thread.sleep(5000);
	  getESUpdate(ids, "customers_test");
	  Map compareMap=new HashMap();
	  //设置需要比对的字段
	  compareMap.put(0,"customer_name;customerName");
	  if(compareMongoAndEs(compareMap)==true)
	  {
		  System.out.println("同步更新成功");
	  }
	  else {
		  System.out.println("同步更新失败");
	  }	
	}		
	
	//更新Customer表的realation_id
	@org.junit.jupiter.api.Test
	void testCustomerUpdateRealationId() throws Exception
	{
	  //测试的mongo表名	
	  String collectionName="sanban_company_customer";
	  Map ids=new HashMap();
	  //需要更新的key和value
	  Map newValue=new HashMap();
	  newValue.put("realation_id", 100);
	  //customer的更新条件需要调整
	  //拿到更新后的数据
	  updateToMongo(collectionName, conditionCusAndSupUpdate, newValue);
	  //拿到更新后的数据
	  mongoRecord=srcDataSource.query(collectionName, conditionCusAndSupUpdate);
	  for(int i=0;i<mongoRecord.size();i++)
	  {
		  ids.put(i+1, mongoRecord.get(i).getData().get("_id"));
	  }
	  Thread.sleep(5000);
	  getESUpdate(ids, "customers_test");
	  Map compareMap=new HashMap();
	  //设置需要比对的字段
	  compareMap.put(0,"realation_id;realationId");
	  if(compareMongoAndEs(compareMap)==true)
	  {
		  System.out.println("同步更新成功");
	  }
	  else {
		  System.out.println("同步更新失败");
	  }	
	}		
	
	//更新Customer表的market_num
	@org.junit.jupiter.api.Test
	void testCustomerUpdateMarketNum() throws Exception
	{
	  //测试的mongo表名	
	  String collectionName="sanban_company_customer";
	  Map ids=new HashMap();
	  //需要更新的key和value
	  Map newValue=new HashMap();
	  newValue.put("market_num", 95785.56);
	  //customer的更新条件需要调整
	  //拿到更新后的数据
	  updateToMongo(collectionName, conditionCusAndSupUpdate, newValue);
	  //拿到更新后的数据
	  mongoRecord=srcDataSource.query(collectionName, conditionCusAndSupUpdate);
	  for(int i=0;i<mongoRecord.size();i++)
	  {
		  ids.put(i+1, mongoRecord.get(i).getData().get("_id"));
	  }
	  Thread.sleep(5000);
	  getESUpdate(ids, "customers_test");
	  Map compareMap=new HashMap();
	  //设置需要比对的字段
	  compareMap.put(0,"market_num;marketNum");
	  if(compareMongoAndEs(compareMap)==true)
	  {
		  System.out.println("同步更新成功");
	  }
	  else {
		  System.out.println("同步更新失败");
	  }	
	}		
	
	//更新Customer表的percent
	@org.junit.jupiter.api.Test
	void testCustomerUpdatePercent() throws Exception
	{
	  //测试的mongo表名	
	  String collectionName="sanban_company_customer";
	  Map ids=new HashMap();
	  //需要更新的key和value
	  Map newValue=new HashMap();
	  newValue.put("percent", "2.99");
	  //customer的更新条件需要调整
	  //拿到更新后的数据
	  updateToMongo(collectionName, conditionCusAndSupUpdate, newValue);
	  //拿到更新后的数据
	  mongoRecord=srcDataSource.query(collectionName, conditionCusAndSupUpdate);
	  for(int i=0;i<mongoRecord.size();i++)
	  {
		  ids.put(i+1, mongoRecord.get(i).getData().get("_id"));
	  }
	  Thread.sleep(5000);
	  getESUpdate(ids, "customers_test");
	  Map compareMap=new HashMap();
	  //设置需要比对的字段
	  compareMap.put(0,"percent;percent");
	  if(compareMongoAndEs(compareMap)==true)
	  {
		  System.out.println("同步更新成功");
	  }
	  else {
		  System.out.println("同步更新失败");
	  }	
	}		
	
	//更新Customer表的content
	@org.junit.jupiter.api.Test
	void testCustomerUpdateContent() throws Exception
	{
	  //测试的mongo表名	
	  String collectionName="sanban_company_customer";
	  Map ids=new HashMap();
	  //需要更新的key和value
	  Map newValue=new HashMap();
	  newValue.put("content", "江南皮革厂倒闭了！！！.");
	  //customer的更新条件需要调整
	  //拿到更新后的数据
	  updateToMongo(collectionName, conditionCusAndSupUpdate, newValue);
	  //拿到更新后的数据
	  mongoRecord=srcDataSource.query(collectionName, conditionCusAndSupUpdate);
	  for(int i=0;i<mongoRecord.size();i++)
	  {
		  ids.put(i+1, mongoRecord.get(i).getData().get("_id"));
	  }
	  Thread.sleep(5000);
	  getESUpdate(ids, "customers_test");
	  Map compareMap=new HashMap();
	  //设置需要比对的字段
	  compareMap.put(0,"content;content");
	  if(compareMongoAndEs(compareMap)==true)
	  {
		  System.out.println("同步更新成功");
	  }
	  else {
		  System.out.println("同步更新失败");
	  }	
	}		
	
	//测试索引sseIndustry
	@org.junit.jupiter.api.Test
	void testSseIndustry() throws IOException, InterruptedException {
	  
  	  String collectionName="industry_grade";
  	  Map conditonal=new HashMap();
  	  conditonal.put("opType", "query");
  	  Map data=new HashMap();
  	  conditonal.put("data", data);
  	  Map filterMap = new HashMap();
  	  Map gtMap  = new HashMap();
  	  gtMap.put("$gt", new ObjectId("000000000000000000000000"));
  	  filterMap.put("_id", gtMap);
  	  Map projectionMap=new HashMap();
  	  projectionMap.put("_id", 0);
  	  data.put("filter", filterMap);
  	  data.put("limit", 3030);
  	  data.put("projection", projectionMap);
  	  //查询数据并新增数据
  	  insertToMongo(collectionName, conditonal);
  	  Thread.sleep(10000);
  	  getESUpdate(ids, "sseindustry_test");
  	  Map compareMap=new HashMap();
  	  //设置需要比对的字段
  	  compareMap.put(0,"encode;code");
  	  compareMap.put(1,"name;name");
  	  compareMap.put(2,"parent_encode;parentEncode");
  	  
  	  //对parent_encode进行必要操作
  	  for(int i=0;i<mongoRecord.size();i++)
  	  {
  		  //parent_encode字段不存在，es中parentEncode=null,如果parent_encode='',parentEncode="顶层行业"
  		  if(mongoRecord.get(i).getData().get("parent_encode")!=null&&mongoRecord.get(i).getData().get("parent_encode").equals(""))
  		  {
  			mongoRecord.get(i).getData().put("parent_encode", "顶层行业");
  		  }
  	  }
  	  if(compareMongoAndEs(compareMap))
  	  {
  		  System.out.println("同步更新成功,测试完成，程序正常");
  	  };
  	  
	}
	
	//更新sseIndustry表的encode
	@org.junit.jupiter.api.Test
	void testSseIndustryUpdateCode() throws Exception
	{
	  //测试的mongo表名	
	  String collectionName="industry_grade";
	  Map ids=new HashMap();
	  //需要更新的key和value
	  Map newValue=new HashMap();
	  newValue.put("encode", 100);
	  //拿到更新后的数据
	  updateToMongo(collectionName, conditionUpdate, newValue);
	  //拿到更新后的数据
	  mongoRecord=srcDataSource.query(collectionName, conditionUpdate);
	  for(int i=0;i<mongoRecord.size();i++)
	  {
		  ids.put(i+1, mongoRecord.get(i).getData().get("_id"));
	  }
	  Thread.sleep(5000);
	  getESUpdate(ids, "sseindustry_test");
	  Map compareMap=new HashMap();
	  //设置需要比对的字段
	  compareMap.put(0,"encode;code");
	  if(compareMongoAndEs(compareMap)==true)
	  {
		  System.out.println("同步更新成功");
	  }
	  else {
		  System.out.println("同步更新失败");
	  }	
	}		
	
	//更新sseIndustry表的name
	@org.junit.jupiter.api.Test
	void testSseIndustryUpdateName() throws Exception
	{
	  //测试的mongo表名	
	  String collectionName="industry_grade";
	  Map ids=new HashMap();
	  //需要更新的key和value
	  Map newValue=new HashMap();
	  newValue.put("name", "自然语言处理");
	  //拿到更新后的数据
	  updateToMongo(collectionName, conditionUpdate, newValue);
	  //拿到更新后的数据
	  mongoRecord=srcDataSource.query(collectionName, conditionUpdate);
	  for(int i=0;i<mongoRecord.size();i++)
	  {
		  ids.put(i+1, mongoRecord.get(i).getData().get("_id"));
	  }
	  Thread.sleep(5000);
	  getESUpdate(ids, "sseindustry_test");
	  Map compareMap=new HashMap();
	  //设置需要比对的字段
	  compareMap.put(0,"name;name");
	  if(compareMongoAndEs(compareMap)==true)
	  {
		  System.out.println("同步更新成功");
	  }
	  else {
		  System.out.println("同步更新失败");
	  }	
	}		

	//更新sseIndustry表的parent_encode
	@org.junit.jupiter.api.Test
	void testSseIndustryUpdateParentEncode() throws Exception
	{
	  //测试的mongo表名	
	  String collectionName="industry_grade";
	  Map ids=new HashMap();
	  //需要更新的key和value
	  Map newValue=new HashMap();
	  newValue.put("parent_encode", "自然语言处理");
	  //拿到更新后的数据
	  updateToMongo(collectionName, conditionUpdate, newValue);
	  //拿到更新后的数据
	  mongoRecord=srcDataSource.query(collectionName, conditionUpdate);
	  for(int i=0;i<mongoRecord.size();i++)
	  {
		  ids.put(i+1, mongoRecord.get(i).getData().get("_id"));
	  }
	  Thread.sleep(5000);
	  getESUpdate(ids, "sseindustry_test");
	  Map compareMap=new HashMap();
	  //设置需要比对的字段
	  compareMap.put(0,"parent_encode;parentEncode");
	  //对parent_encode进行必要操作
  	  for(int i=0;i<mongoRecord.size();i++)
  	  {
  		  //parent_encode字段不存在，es中parentEncode=null,如果parent_encode='',parentEncode="顶层行业"
  		  if(mongoRecord.get(i).getData().get("parent_encode")!=null&&mongoRecord.get(i).getData().get("parent_encode").equals(""))
  		  {
  			mongoRecord.get(i).getData().put("parent_encode", "顶层行业");
  		  }
  	  }
	  if(compareMongoAndEs(compareMap)==true)
	  {
		  System.out.println("同步更新成功");
	  }
	  else {
		  System.out.println("同步更新失败");
	  }	
	}			
	
	//测试索引InvestCount
	@org.junit.jupiter.api.Test
	void testInvestCount() throws IOException, InterruptedException {
	  
  	  String collectionName="investtrade";
  	  Map conditonal=new HashMap();
  	  conditonal.put("opType", "query");
  	  Map data=new HashMap();
  	  conditonal.put("data", data);
  	  Map filterMap = new HashMap();
  	  Map gtMap  = new HashMap();
  	  gtMap.put("$gt", new ObjectId("000000000000000000000000"));
  	  filterMap.put("_id", gtMap);
  	  Map projectionMap=new HashMap();
  	  projectionMap.put("_id", 0);
  	  data.put("filter", filterMap);
  	  data.put("limit", 10);
  	  data.put("projection", projectionMap);
  	  //查询数据并新增数据
  	  insertToMongo(collectionName, conditonal);
  	  Thread.sleep(20000);
  	  getESUpdate(ids, "investcount_test");
  	  /*
  	  //对获取到的ES的数据进行必要的修正,如先不检测capitalAmount和industryLevel1st字段
  	  for(int i=0;i<ids.size();i++)
  	  {
  		  Map source=(Map) esRecord.get(ids.get(i+1));
  		  source.remove("capitalAmount");
  		  source.remove("industryLevel1st");
  	  }*/  
  	  Map compareMap=new HashMap();
  	  //设置需要比对的字段
  	  compareMap.put(0,"company_ids;companyId");
  	  compareMap.put(1,"sse_industry;industry");
  	  compareMap.put(2,"step;round");
  	  compareMap.put(3,"steptime;date");
  	  compareMap.put(4,"fullname;fullname");
  	  compareMap.put(5,"shortname;alternativeName");
  	  compareMap.put(6,"tag;sseTag");
  	  compareMap.put(7,"investor_id;investorsId");
  	  compareMap.put(8,"investors;investorsShortname");

  	  if(compareMongoAndEs(compareMap))
  	  {
  		  System.out.println("同步更新成功,测试完成，程序正常");
  	  };
  	  
	}
	
	//更新InvestCount表的company_id
	@org.junit.jupiter.api.Test
	void testInvestCountUpdateCompanyId() throws Exception
	{
	  //测试的mongo表名	
	  String collectionName="investtrade";
	  Map ids=new HashMap();
	  //需要更新的key和value
	  Map newValue=new HashMap();
	  newValue.put("company_id", 1000);
	  //拿到更新后的数据
	  updateToMongo(collectionName, conditionUpdate, newValue);
	  //拿到更新后的数据
	  mongoRecord=srcDataSource.query(collectionName, conditionUpdate);
	  for(int i=0;i<mongoRecord.size();i++)
	  {
		  ids.put(i+1, mongoRecord.get(i).getData().get("_id"));
	  }
	  Thread.sleep(5000);
	  getESUpdate(ids, "investcount_test");
	  Map compareMap=new HashMap();
	  //设置需要比对的字段
	  compareMap.put(0,"company_id;companyId");
	  if(compareMongoAndEs(compareMap)==true)
	  {
		  System.out.println("同步更新成功");
	  }
	  else {
		  System.out.println("同步更新失败");
	  }	
	}		
	
	//更新InvestCount表的sse_industry
	@org.junit.jupiter.api.Test
	void testInvestCountUpdateSseIndustry() throws Exception
	{
	  //测试的mongo表名	
	  String collectionName="investtrade";
	  Map ids=new HashMap();
	  //需要更新的key和value
	  Map newValue=new HashMap();
	  newValue.put("sse_industry", "F02010101");
	  //拿到更新后的数据
	  updateToMongo(collectionName, conditionUpdate, newValue);
	  //拿到更新后的数据
	  mongoRecord=srcDataSource.query(collectionName, conditionUpdate);
	  for(int i=0;i<mongoRecord.size();i++)
	  {
		  ids.put(i+1, mongoRecord.get(i).getData().get("_id"));
	  }
	  Thread.sleep(5000);
	  getESUpdate(ids, "investcount_test");
	  Map compareMap=new HashMap();
	  //设置需要比对的字段
	  compareMap.put(0,"sse_industry;industry");
	  if(compareMongoAndEs(compareMap)==true)
	  {
		  System.out.println("同步更新成功");
	  }
	  else {
		  System.out.println("同步更新失败");
	  }	
	}		
	
	//更新InvestCount表的steptime
	@org.junit.jupiter.api.Test
	void testInvestCountUpdateSteptime() throws Exception
	{
	  //测试的mongo表名	
	  String collectionName="investtrade";
	  Map ids=new HashMap();
	  //需要更新的key和value
	  Map newValue=new HashMap();
	  newValue.put("steptime", "2018-05-29");
	  //拿到更新后的数据
	  updateToMongo(collectionName, conditionUpdate, newValue);
	  //拿到更新后的数据
	  mongoRecord=srcDataSource.query(collectionName, conditionUpdate);
	  for(int i=0;i<mongoRecord.size();i++)
	  {
		  ids.put(i+1, mongoRecord.get(i).getData().get("_id"));
	  }
	  Thread.sleep(5000);
	  getESUpdate(ids, "investcount_test");
	  Map compareMap=new HashMap();
	  //设置需要比对的字段
	  compareMap.put(0,"steptime;date");
	  if(compareMongoAndEs(compareMap)==true)
	  {
		  System.out.println("同步更新成功");
	  }
	  else {
		  System.out.println("同步更新失败");
	  }	
	}		

	//更新InvestCount表的fullname
	@org.junit.jupiter.api.Test
	void testInvestCountUpdateFullname() throws Exception
	{
	  //测试的mongo表名	
	  String collectionName="investtrade";
	  Map ids=new HashMap();
	  //需要更新的key和value
	  Map newValue=new HashMap();
	  newValue.put("fullname", "昆明滇虹药业集团有限公司");
	  //拿到更新后的数据
	  updateToMongo(collectionName, conditionUpdate, newValue);
	  //拿到更新后的数据
	  mongoRecord=srcDataSource.query(collectionName, conditionUpdate);
	  for(int i=0;i<mongoRecord.size();i++)
	  {
		  ids.put(i+1, mongoRecord.get(i).getData().get("_id"));
	  }
	  Thread.sleep(5000);
	  getESUpdate(ids, "investcount_test");
	  Map compareMap=new HashMap();
	  //设置需要比对的字段
	  compareMap.put(0,"fullname;fullname");
	  if(compareMongoAndEs(compareMap)==true)
	  {
		  System.out.println("同步更新成功");
	  }
	  else {
		  System.out.println("同步更新失败");
	  }	
	}		
	
	//更新InvestCount表的shortname
	@org.junit.jupiter.api.Test
	void testInvestCountUpdateAlternativeName() throws Exception
	{
	  //测试的mongo表名	
	  String collectionName="investtrade";
	  Map ids=new HashMap();
	  //需要更新的key和value
	  Map newValue=new HashMap();
	  newValue.put("shortname", "深圳证券");
	  //拿到更新后的数据
	  updateToMongo(collectionName, conditionUpdate, newValue);
	  //拿到更新后的数据
	  mongoRecord=srcDataSource.query(collectionName, conditionUpdate);
	  for(int i=0;i<mongoRecord.size();i++)
	  {
		  ids.put(i+1, mongoRecord.get(i).getData().get("_id"));
	  }
	  Thread.sleep(5000);
	  getESUpdate(ids, "investcount_test");
	  Map compareMap=new HashMap();
	  //设置需要比对的字段
	  compareMap.put(0,"shortname;alternativeName");
	  if(compareMongoAndEs(compareMap)==true)
	  {
		  System.out.println("同步更新成功");
	  }
	  else {
		  System.out.println("同步更新失败");
	  }	
	}		
	
	//更新InvestCount表的tag
	@org.junit.jupiter.api.Test
	void testInvestCountUpdateSseTag() throws Exception
	{
	  //测试的mongo表名	
	  String collectionName="investtrade";
	  Map ids=new HashMap();
	  //需要更新的key和value
	  Map newValue=new HashMap();
	  List tags=new ArrayList();
	  tags.add("医疗器械");
	  tags.add("专用设备");
	  newValue.put("tag", tags);
	  //拿到更新后的数据
	  updateToMongo(collectionName, conditionUpdate, newValue);
	  //拿到更新后的数据
	  mongoRecord=srcDataSource.query(collectionName, conditionUpdate);
	  for(int i=0;i<mongoRecord.size();i++)
	  {
		  ids.put(i+1, mongoRecord.get(i).getData().get("_id"));
	  }
	  Thread.sleep(5000);
	  getESUpdate(ids, "investcount_test");
	  Map compareMap=new HashMap();
	  //设置需要比对的字段
	  compareMap.put(0,"tag;sseTag");
	  if(compareMongoAndEs(compareMap)==true)
	  {
		  System.out.println("同步更新成功");
	  }
	  else {
		  System.out.println("同步更新失败");
	  }	
	}		
	
	//更新InvestCount表的investor_id
	@org.junit.jupiter.api.Test
	void testInvestCountUpdateInvestorId() throws Exception
	{
	  //测试的mongo表名	
	  String collectionName="investtrade";
	  Map ids=new HashMap();
	  //需要更新的key和value
	  Map newValue=new HashMap();
	  List investorIds=new ArrayList();
	  investorIds.add(1484688);
	  investorIds.add(84688);
	  newValue.put("investor_id", investorIds);
	  //拿到更新后的数据
	  updateToMongo(collectionName, conditionUpdate, newValue);
	  //拿到更新后的数据
	  mongoRecord=srcDataSource.query(collectionName, conditionUpdate);
	  for(int i=0;i<mongoRecord.size();i++)
	  {
		  ids.put(i+1, mongoRecord.get(i).getData().get("_id"));
	  }
	  Thread.sleep(5000);
	  getESUpdate(ids, "investcount_test");
	  Map compareMap=new HashMap();
	  //设置需要比对的字段
	  compareMap.put(0,"investor_id;investorId");
	  if(compareMongoAndEs(compareMap)==true)
	  {
		  System.out.println("同步更新成功");
	  }
	  else {
		  System.out.println("同步更新失败");
	  }	
	}		
	
	//更新InvestCount表的investors
	@org.junit.jupiter.api.Test
	void testInvestCountUpdateInvestorsShortname() throws Exception
	{
	  //测试的mongo表名	
	  String collectionName="investtrade";
	  Map ids=new HashMap();
	  //需要更新的key和value
	  Map newValue=new HashMap();
	  List investors=new ArrayList();
	  investors.add("青岛科投");
	  investors.add("九鼎投资");
	  newValue.put("investors", investors);
	  //拿到更新后的数据
	  updateToMongo(collectionName, conditionUpdate, newValue);
	  //拿到更新后的数据
	  mongoRecord=srcDataSource.query(collectionName, conditionUpdate);
	  for(int i=0;i<mongoRecord.size();i++)
	  {
		  ids.put(i+1, mongoRecord.get(i).getData().get("_id"));
	  }
	  Thread.sleep(5000);
	  getESUpdate(ids, "investcount_test");
	  Map compareMap=new HashMap();
	  //设置需要比对的字段
	  compareMap.put(0,"investors;investorsShortname");
	  if(compareMongoAndEs(compareMap)==true)
	  {
		  System.out.println("同步更新成功");
	  }
	  else {
		  System.out.println("同步更新失败");
	  }	
	}		

	//还没有测试
	//测试索引Invests
	@org.junit.jupiter.api.Test
	void testInvests() throws IOException, InterruptedException {
	  
  	  String collectionName="sanban_company_holder";
  	  Map conditonal=new HashMap();
  	  conditonal.put("opType", "query");
  	  Map data=new HashMap();
  	  conditonal.put("data", data);
  	  Map filterMap = new HashMap();
  	  Map gtMap  = new HashMap();
  	  gtMap.put("$gt", new ObjectId("000000000000000000000000"));
  	  filterMap.put("_id", gtMap);
  	  Map projectionMap=new HashMap();
  	  projectionMap.put("_id", 0);
  	  data.put("filter", filterMap);
  	  data.put("limit", 10);
  	  data.put("projection", projectionMap);
  	  //查询数据并新增数据
  	  insertToMongo(collectionName, conditonal);
  	  Thread.sleep(10000);
  	  getESUpdate(ids, "invests_test");	  
  	  Map compareMap=new HashMap();
  	  //设置需要比对的字段
  	  compareMap.put(0,"company_id;companyId");
  	  compareMap.put(1,"realation_id;investedId");
  	  compareMap.put(2,"crash_num;amount");
  	  compareMap.put(3,"percent;percent");
  	  compareMap.put(4,"type;type");
  	  compareMap.put(5,"partner;partner");
  	  compareMap.put(6,"person_id;personId");
  	  if(compareMongoAndEs(compareMap))
  	  {
  		  System.out.println("同步更新成功,测试完成，程序正常");
  	  };
  	  
	}
	
	//更新Invests表的company_id
	@org.junit.jupiter.api.Test
	void testInvestsUpdateCompanyId() throws Exception
	{
	  //测试的mongo表名	
	  String collectionName="sanban_company_holder";
	  Map ids=new HashMap();
	  //需要更新的key和value
	  Map newValue=new HashMap();
	  newValue.put("company_id", 1000);
	  //拿到更新后的数据
	  updateToMongo(collectionName, conditionUpdate, newValue);
	  //拿到更新后的数据
	  mongoRecord=srcDataSource.query(collectionName, conditionUpdate);
	  for(int i=0;i<mongoRecord.size();i++)
	  {
		  ids.put(i+1, mongoRecord.get(i).getData().get("_id"));
	  }
	  Thread.sleep(5000);
	  getESUpdate(ids, "invests_test");
	  Map compareMap=new HashMap();
	  //设置需要比对的字段
	  compareMap.put(0,"company_id;companyId");
	  if(compareMongoAndEs(compareMap)==true)
	  {
		  System.out.println("同步更新成功");
	  }
	  else {
		  System.out.println("同步更新失败");
	  }	
	}		
	
	//更新Invests表的realation_id
	@org.junit.jupiter.api.Test
	void testInvestsUpdateRealationId() throws Exception
	{
	  //测试的mongo表名	
	  String collectionName="sanban_company_holder";
	  Map ids=new HashMap();
	  //需要更新的key和value
	  Map newValue=new HashMap();
	  newValue.put("realation_id", 1000);
	  //拿到更新后的数据
	  updateToMongo(collectionName, conditionUpdate, newValue);
	  //拿到更新后的数据
	  mongoRecord=srcDataSource.query(collectionName, conditionUpdate);
	  for(int i=0;i<mongoRecord.size();i++)
	  {
		  ids.put(i+1, mongoRecord.get(i).getData().get("_id"));
	  }
	  Thread.sleep(5000);
	  getESUpdate(ids, "invests_test");
	  Map compareMap=new HashMap();
	  //设置需要比对的字段
	  compareMap.put(0,"realation_id;realationId");
	  if(compareMongoAndEs(compareMap)==true)
	  {
		  System.out.println("同步更新成功");
	  }
	  else {
		  System.out.println("同步更新失败");
	  }	
	}		
	
	//更新Invests表的crash_num
	@org.junit.jupiter.api.Test
	void testInvestsUpdateAmount() throws Exception
	{
	  //测试的mongo表名	
	  String collectionName="sanban_company_holder";
	  Map ids=new HashMap();
	  //需要更新的key和value
	  Map newValue=new HashMap();
	  newValue.put("crash_num", "78.26万元");
	  //拿到更新后的数据
	  updateToMongo(collectionName, conditionUpdate, newValue);
	  //拿到更新后的数据
	  mongoRecord=srcDataSource.query(collectionName, conditionUpdate);
	  for(int i=0;i<mongoRecord.size();i++)
	  {
		  ids.put(i+1, mongoRecord.get(i).getData().get("_id"));
	  }
	  Thread.sleep(5000);
	  getESUpdate(ids, "invests_test");
	  Map compareMap=new HashMap();
	  //设置需要比对的字段
	  compareMap.put(0,"crash_num;amount");
	  if(compareMongoAndEs(compareMap)==true)
	  {
		  System.out.println("同步更新成功");
	  }
	  else {
		  System.out.println("同步更新失败");
	  }	
	}		
	
	//更新Invests表的percent
	@org.junit.jupiter.api.Test
	void testInvestsUpdatePercent() throws Exception
	{
	  //测试的mongo表名	
	  String collectionName="sanban_company_holder";
	  Map ids=new HashMap();
	  //需要更新的key和value
	  Map newValue=new HashMap();
	  newValue.put("percent", "8.99%");
	  //拿到更新后的数据
	  updateToMongo(collectionName, conditionUpdate, newValue);
	  //拿到更新后的数据
	  mongoRecord=srcDataSource.query(collectionName, conditionUpdate);
	  for(int i=0;i<mongoRecord.size();i++)
	  {
		  ids.put(i+1, mongoRecord.get(i).getData().get("_id"));
	  }
	  Thread.sleep(5000);
	  getESUpdate(ids, "invests_test");
	  Map compareMap=new HashMap();
	  //设置需要比对的字段
	  compareMap.put(0,"percent;percent");
	  if(compareMongoAndEs(compareMap)==true)
	  {
		  System.out.println("同步更新成功");
	  }
	  else {
		  System.out.println("同步更新失败");
	  }	
	}		

	//更新Invests表的type
	@org.junit.jupiter.api.Test
	void testInvestsUpdateType() throws Exception
	{
	  //测试的mongo表名	
	  String collectionName="sanban_company_holder";
	  Map ids=new HashMap();
	  //需要更新的key和value
	  Map newValue=new HashMap();
	  newValue.put("type", "法人股东");
	  //拿到更新后的数据
	  updateToMongo(collectionName, conditionUpdate, newValue);
	  //拿到更新后的数据
	  mongoRecord=srcDataSource.query(collectionName, conditionUpdate);
	  for(int i=0;i<mongoRecord.size();i++)
	  {
		  ids.put(i+1, mongoRecord.get(i).getData().get("_id"));
	  }
	  Thread.sleep(5000);
	  getESUpdate(ids, "invests_test");
	  Map compareMap=new HashMap();
	  //设置需要比对的字段
	  compareMap.put(0,"type;type");
	  if(compareMongoAndEs(compareMap)==true)
	  {
		  System.out.println("同步更新成功");
	  }
	  else {
		  System.out.println("同步更新失败");
	  }	
	}		
	
	//更新Invests表的partener
	@org.junit.jupiter.api.Test
	void testInvestsUpdatePartener() throws Exception
	{
	  //测试的mongo表名	
	  String collectionName="sanban_company_holder";
	  Map ids=new HashMap();
	  //需要更新的key和value
	  Map newValue=new HashMap();
	  newValue.put("partener", "唯优印象（北京）国际文化传媒股份有限公司");
	  //拿到更新后的数据
	  updateToMongo(collectionName, conditionUpdate, newValue);
	  //拿到更新后的数据
	  mongoRecord=srcDataSource.query(collectionName, conditionUpdate);
	  for(int i=0;i<mongoRecord.size();i++)
	  {
		  ids.put(i+1, mongoRecord.get(i).getData().get("_id"));
	  }
	  Thread.sleep(5000);
	  getESUpdate(ids, "invests_test");
	  Map compareMap=new HashMap();
	  //设置需要比对的字段
	  compareMap.put(0,"partener;partener");
	  if(compareMongoAndEs(compareMap)==true)
	  {
		  System.out.println("同步更新成功");
	  }
	  else {
		  System.out.println("同步更新失败");
	  }	
	}		
	
	//更新Invests表的person_id
	@org.junit.jupiter.api.Test
	void testInvestsUpdatePersonId() throws Exception
	{
	  //测试的mongo表名	
	  String collectionName="sanban_company_holder";
	  Map ids=new HashMap();
	  //需要更新的key和value
	  Map newValue=new HashMap();
	  newValue.put("person_id", 3417251);
	  //拿到更新后的数据
	  updateToMongo(collectionName, conditionUpdate, newValue);
	  //拿到更新后的数据
	  mongoRecord=srcDataSource.query(collectionName, conditionUpdate);
	  for(int i=0;i<mongoRecord.size();i++)
	  {
		  ids.put(i+1, mongoRecord.get(i).getData().get("_id"));
	  }
	  Thread.sleep(5000);
	  getESUpdate(ids, "invests_test");
	  Map compareMap=new HashMap();
	  //设置需要比对的字段
	  compareMap.put(0,"person_id;personId");
	  if(compareMongoAndEs(compareMap)==true)
	  {
		  System.out.println("同步更新成功");
	  }
	  else {
		  System.out.println("同步更新失败");
	  }	
	}		
	
	//还没有测试
	//测试索引InvestInstitution
	@org.junit.jupiter.api.Test
	void testInvestInstitution() throws IOException, InterruptedException {
	  
  	  String collectionName="invest_institution";
  	  Map conditonal=new HashMap();
  	  conditonal.put("opType", "query");
  	  Map data=new HashMap();
  	  conditonal.put("data", data);
  	  Map filterMap = new HashMap();
  	  Map gtMap  = new HashMap();
  	  gtMap.put("$gt", new ObjectId("000000000000000000000000"));
  	  filterMap.put("_id", gtMap);
  	  Map projectionMap=new HashMap();
  	  projectionMap.put("_id", 0);
  	  data.put("filter", filterMap);
  	  data.put("limit", 50);
  	  data.put("projection", projectionMap);
  	  //查询数据并新增数据
  	  insertToMongo(collectionName, conditonal);
  	  Thread.sleep(30000);
  	  getESUpdate(ids, "investinstitution_test");	  
  	  Map compareMap=new HashMap();
  	  //设置需要比对的字段
  	  compareMap.put(0,"company_id;companyId");
  	  //compareMap.put(1,"company_name;companyName");
  	  compareMap.put(1,"invest_round;round");
  	  compareMap.put(2,"invest_field;field");
  	  compareMap.put(3,"introduction;description");
      compareMap.put(4,"capital_scale_count;capitalScale");
  	  compareMap.put(5,"single_company_invest_scale;singleInvestScale");
  	  //对获取到的ES的数据进行必要的修正,如先不检测capitalAmount和industryLevel1st字段
  	  for(int i=0;i<ids.size();i++)
  	  {
  		  Map source=(Map) esRecord.get(ids.get(i+1));
  		  source.remove("person");
  	  } 	
  	  if(compareMongoAndEs(compareMap))
  	  {
  		  System.out.println("同步更新成功,测试完成，程序正常");
  	  };
  	  
	}
	
	//更新Invests表的company_id
	@org.junit.jupiter.api.Test
	void testInvestInstitutionUpdateCompanyId() throws Exception
	{
	  //测试的mongo表名	
	  String collectionName="invest_institution";
	  Map ids=new HashMap();
	  //需要更新的key和value
	  Map newValue=new HashMap();
	  newValue.put("company_id", 1000);
	  //拿到更新后的数据
	  updateToMongo(collectionName, conditionUpdate, newValue);
	  //拿到更新后的数据
	  mongoRecord=srcDataSource.query(collectionName, conditionUpdate);
	  for(int i=0;i<mongoRecord.size();i++)
	  {
		  ids.put(i+1, mongoRecord.get(i).getData().get("_id"));
	  }
	  Thread.sleep(5000);
	  getESUpdate(ids, "invests_test");
	  Map compareMap=new HashMap();
	  //设置需要比对的字段
	  compareMap.put(0,"company_id;companyId");
	  if(compareMongoAndEs(compareMap)==true)
	  {
		  System.out.println("同步更新成功");
	  }
	  else {
		  System.out.println("同步更新失败");
	  }	
	}		
	
	//更新Invests表的invest_round
	@org.junit.jupiter.api.Test
	void testInvestInstitutionUpdateRound() throws Exception
	{
	  //测试的mongo表名	
	  String collectionName="invest_institution";
	  Map ids=new HashMap();
	  //需要更新的key和value
	  Map newValue=new HashMap();
	  newValue.put("invest_round", "A轮,B轮,C轮,Pre-A");
	  //拿到更新后的数据
	  updateToMongo(collectionName, conditionUpdate, newValue);
	  //拿到更新后的数据
	  mongoRecord=srcDataSource.query(collectionName, conditionUpdate);
	  for(int i=0;i<mongoRecord.size();i++)
	  {
		  ids.put(i+1, mongoRecord.get(i).getData().get("_id"));
	  }
	  Thread.sleep(5000);
	  getESUpdate(ids, "invests_test");
	  Map compareMap=new HashMap();
	  //设置需要比对的字段
	  compareMap.put(0,"invest_round;round");
	  if(compareMongoAndEs(compareMap)==true)
	  {
		  System.out.println("同步更新成功");
	  }
	  else {
		  System.out.println("同步更新失败");
	  }	
	}		
	
	//更新Invests表的invest_field
	@org.junit.jupiter.api.Test
	void testInvestInstitutionUpdateField() throws Exception
	{
	  //测试的mongo表名	
	  String collectionName="invest_institution";
	  Map ids=new HashMap();
	  //需要更新的key和value
	  Map newValue=new HashMap();
	  newValue.put("invest_field", "系统集成,应用软件,电子商务");
	  //拿到更新后的数据
	  updateToMongo(collectionName, conditionUpdate, newValue);
	  //拿到更新后的数据
	  mongoRecord=srcDataSource.query(collectionName, conditionUpdate);
	  for(int i=0;i<mongoRecord.size();i++)
	  {
		  ids.put(i+1, mongoRecord.get(i).getData().get("_id"));
	  }
	  Thread.sleep(5000);
	  getESUpdate(ids, "invests_test");
	  Map compareMap=new HashMap();
	  //设置需要比对的字段
	  compareMap.put(0,"invest_field;field");
	  if(compareMongoAndEs(compareMap)==true)
	  {
		  System.out.println("同步更新成功");
	  }
	  else {
		  System.out.println("同步更新失败");
	  }	
	}		
	
	//更新Invests表的description
	@org.junit.jupiter.api.Test
	void testInvestInstitutionUpdateDescription() throws Exception
	{
	  //测试的mongo表名	
	  String collectionName="invest_institution";
	  Map ids=new HashMap();
	  //需要更新的key和value
	  Map newValue=new HashMap();
	  newValue.put("description", "系统集成,应用软件,电子商务");
	  //拿到更新后的数据
	  updateToMongo(collectionName, conditionUpdate, newValue);
	  //拿到更新后的数据
	  mongoRecord=srcDataSource.query(collectionName, conditionUpdate);
	  for(int i=0;i<mongoRecord.size();i++)
	  {
		  ids.put(i+1, mongoRecord.get(i).getData().get("_id"));
	  }
	  Thread.sleep(5000);
	  getESUpdate(ids, "invests_test");
	  Map compareMap=new HashMap();
	  //设置需要比对的字段
	  compareMap.put(0,"description;description");
	  if(compareMongoAndEs(compareMap)==true)
	  {
		  System.out.println("同步更新成功");
	  }
	  else {
		  System.out.println("同步更新失败");
	  }	
	}		
	
	//更新Invests表的capital_scale_count
	@org.junit.jupiter.api.Test
	void testInvestInstitutionUpdateCapitalScale() throws Exception
	{
	  //测试的mongo表名	
	  String collectionName="invest_institution";
	  Map ids=new HashMap();
	  //需要更新的key和value
	  Map newValue=new HashMap();
	  newValue.put("capital_scale_count", "55");
	  //拿到更新后的数据
	  updateToMongo(collectionName, conditionUpdate, newValue);
	  //拿到更新后的数据
	  mongoRecord=srcDataSource.query(collectionName, conditionUpdate);
	  for(int i=0;i<mongoRecord.size();i++)
	  {
		  ids.put(i+1, mongoRecord.get(i).getData().get("_id"));
	  }
	  Thread.sleep(5000);
	  getESUpdate(ids, "invests_test");
	  Map compareMap=new HashMap();
	  //设置需要比对的字段
	  compareMap.put(0,"capital_scale_count;capitalScale");
	  if(compareMongoAndEs(compareMap)==true)
	  {
		  System.out.println("同步更新成功");
	  }
	  else {
		  System.out.println("同步更新失败");
	  }	
	}		
	
	//更新Invests表的single_company_invest_scale
	@org.junit.jupiter.api.Test
	void testInvestInstitutionUpdateSingleInvestScale() throws Exception
	{
	  //测试的mongo表名	
	  String collectionName="invest_institution";
	  Map ids=new HashMap();
	  //需要更新的key和value
	  Map newValue=new HashMap();
	  newValue.put("single_company_invest_scale", "55");
	  //拿到更新后的数据
	  updateToMongo(collectionName, conditionUpdate, newValue);
	  //拿到更新后的数据
	  mongoRecord=srcDataSource.query(collectionName, conditionUpdate);
	  for(int i=0;i<mongoRecord.size();i++)
	  {
		  ids.put(i+1, mongoRecord.get(i).getData().get("_id"));
	  }
	  Thread.sleep(5000);
	  getESUpdate(ids, "invests_test");
	  Map compareMap=new HashMap();
	  //设置需要比对的字段
	  compareMap.put(0,"single_company_invest_scale;singleInvestScale");
	  if(compareMongoAndEs(compareMap)==true)
	  {
		  System.out.println("同步更新成功");
	  }
	  else {
		  System.out.println("同步更新失败");
	  }	
	}		
	
	//测试索引changes
	@org.junit.jupiter.api.Test
	void testChanges() throws IOException, InterruptedException {
	  
  	  String collectionName="sanban_company_change";
  	  Map conditonal=new HashMap();
  	  conditonal.put("opType", "query");
  	  Map data=new HashMap();
  	  conditonal.put("data", data);
  	  Map filterMap = new HashMap();
  	  Map gtMap  = new HashMap();
  	  gtMap.put("$gt", new ObjectId("000000000000000000000000"));
  	  filterMap.put("_id", gtMap);
  	  Map projectionMap=new HashMap();
  	  projectionMap.put("_id", 0);
  	  data.put("filter", filterMap);
  	  data.put("limit", 1000);
  	  data.put("projection", projectionMap);
  	  //查询数据并新增数据
  	  insertToMongo(collectionName, conditonal);
  	  Thread.sleep(10000);
  	  getESUpdate(ids, "changes_test");	  
  	  Map compareMap=new HashMap();
  	  //设置需要比对的字段
  	  compareMap.put(0,"company_id;companyId");
  	  compareMap.put(1,"bgsj;changeTime");
  	  compareMap.put(2,"bgxm;changeItem");
  	  compareMap.put(3,"bgq;original");
  	  compareMap.put(4,"bgh;changed");
  	  if(compareMongoAndEs(compareMap))
  	  {
  		  System.out.println("同步更新成功,测试完成，程序正常");
  	  };
  	  
	}
	
	//更新changes表的company_id
	@org.junit.jupiter.api.Test
	void testChangesUpdateCompanyId() throws Exception
	{
	  //测试的mongo表名	
	  String collectionName="sanban_company_change";
	  Map ids=new HashMap();
	  //需要更新的key和value
	  Map newValue=new HashMap();
	  newValue.put("company_id", 100);
	  //拿到更新后的数据
	  updateToMongo(collectionName, conditionUpdate, newValue);
	  //拿到更新后的数据
	  mongoRecord=srcDataSource.query(collectionName, conditionUpdate);
	  for(int i=0;i<mongoRecord.size();i++)
	  {
		  ids.put(i+1, mongoRecord.get(i).getData().get("_id"));
	  }
	  Thread.sleep(5000);
	  getESUpdate(ids, "changes_test");
	  Map compareMap=new HashMap();
	  //设置需要比对的字段
	  compareMap.put(0,"company_id;companyId");
	  if(compareMongoAndEs(compareMap)==true)
	  {
		  System.out.println("同步更新成功");
	  }
	  else {
		  System.out.println("同步更新失败");
	  }	
	}		
	
	//更新changes表的bgsj
	@org.junit.jupiter.api.Test
	void testChangesUpdateChangeTime() throws Exception
	{
	  //测试的mongo表名	
	  String collectionName="sanban_company_change";
	  Map ids=new HashMap();
	  //需要更新的key和value
	  Map newValue=new HashMap();
	  newValue.put("bgsj", "2018-05-31");
	  //拿到更新后的数据
	  updateToMongo(collectionName, conditionUpdate, newValue);
	  //拿到更新后的数据
	  mongoRecord=srcDataSource.query(collectionName, conditionUpdate);
	  for(int i=0;i<mongoRecord.size();i++)
	  {
		  ids.put(i+1, mongoRecord.get(i).getData().get("_id"));
	  }
	  Thread.sleep(5000);
	  getESUpdate(ids, "changes_test");
	  Map compareMap=new HashMap();
	  //设置需要比对的字段
	  compareMap.put(0,"bgsj;changeTime");
	  if(compareMongoAndEs(compareMap)==true)
	  {
		  System.out.println("同步更新成功");
	  }
	  else {
		  System.out.println("同步更新失败");
	  }	
	}		
	
	//更新changes表的bgxm
	@org.junit.jupiter.api.Test
	void testChangesUpdateChangeItem() throws Exception
	{
	  //测试的mongo表名	
	  String collectionName="sanban_company_change";
	  Map ids=new HashMap();
	  //需要更新的key和value
	  Map newValue=new HashMap();
	  newValue.put("bgxm", "经营范围");
	  //拿到更新后的数据
	  updateToMongo(collectionName, conditionUpdate, newValue);
	  //拿到更新后的数据
	  mongoRecord=srcDataSource.query(collectionName, conditionUpdate);
	  for(int i=0;i<mongoRecord.size();i++)
	  {
		  ids.put(i+1, mongoRecord.get(i).getData().get("_id"));
	  }
	  Thread.sleep(5000);
	  getESUpdate(ids, "changes_test");
	  Map compareMap=new HashMap();
	  //设置需要比对的字段
	  compareMap.put(0,"bgxm;changeItem");
	  if(compareMongoAndEs(compareMap)==true)
	  {
		  System.out.println("同步更新成功");
	  }
	  else {
		  System.out.println("同步更新失败");
	  }	
	}		
	
	//更新changes表的bgq
	@org.junit.jupiter.api.Test
	void testChangesUpdateoOriginal() throws Exception
	{
	  //测试的mongo表名	
	  String collectionName="sanban_company_change";
	  Map ids=new HashMap();
	  //需要更新的key和value
	  Map newValue=new HashMap();
	  newValue.put("bgq", "物联网的技术开发与技术服务信息技术开发、技术转让、技术咨询、技术服务；机械设备开发计算机系统集成软件开发销售机械设备机械设备租赁。");
	  //拿到更新后的数据
	  updateToMongo(collectionName, conditionUpdate, newValue);
	  //拿到更新后的数据
	  mongoRecord=srcDataSource.query(collectionName, conditionUpdate);
	  for(int i=0;i<mongoRecord.size();i++)
	  {
		  ids.put(i+1, mongoRecord.get(i).getData().get("_id"));
	  }
	  Thread.sleep(5000);
	  getESUpdate(ids, "changes_test");
	  Map compareMap=new HashMap();
	  //设置需要比对的字段
	  compareMap.put(0,"bgq;original");
	  if(compareMongoAndEs(compareMap)==true)
	  {
		  System.out.println("同步更新成功");
	  }
	  else {
		  System.out.println("同步更新失败");
	  }	
	}		
	
	//更新changes表的bgh
	@org.junit.jupiter.api.Test
	void testChangesUpdateoChanged() throws Exception
	{
	  //测试的mongo表名	
	  String collectionName="sanban_company_change";
	  Map ids=new HashMap();
	  //需要更新的key和value
	  Map newValue=new HashMap();
	  newValue.put("bgh", "物联网的技术开发与技术服务信息技术开发、技术转让、技术咨询、技术服务；机械设备开发计算机系统集成软件开发销售机械设备机械设备租赁。");
	  //拿到更新后的数据
	  updateToMongo(collectionName, conditionUpdate, newValue);
	  //拿到更新后的数据
	  mongoRecord=srcDataSource.query(collectionName, conditionUpdate);
	  for(int i=0;i<mongoRecord.size();i++)
	  {
		  ids.put(i+1, mongoRecord.get(i).getData().get("_id"));
	  }
	  Thread.sleep(5000);
	  getESUpdate(ids, "changes_test");
	  Map compareMap=new HashMap();
	  //设置需要比对的字段
	  compareMap.put(0,"bgh;changed");
	  if(compareMongoAndEs(compareMap)==true)
	  {
		  System.out.println("同步更新成功");
	  }
	  else {
		  System.out.println("同步更新失败");
	  }	
	}		
	
	//测试索引Legalinstrument
	@org.junit.jupiter.api.Test
	void testLegalinstrument() throws IOException, InterruptedException {
	  
  	  String collectionName="tb_wenshu";
  	  Map conditonal=new HashMap();
  	  conditonal.put("opType", "query");
  	  Map data=new HashMap();
  	  conditonal.put("data", data);
  	  Map filterMap = new HashMap();
  	  Map gtMap  = new HashMap();
  	  gtMap.put("$gt", new ObjectId("000000000000000000000000"));
  	  filterMap.put("_id", gtMap);
  	  Map projectionMap=new HashMap();
  	  projectionMap.put("_id", 0);
  	  data.put("filter", filterMap);
  	  data.put("limit", 10);
  	  data.put("projection", projectionMap);
  	  //查询数据并新增数据
  	  insertToMongo(collectionName, conditonal);
  	  operateRecord(mongoRecord);
  	  Thread.sleep(15000);
  	  getESUpdate(ids, "legalinstrument_test");	  
  	  Map compareMap=new HashMap();
  	  //设置需要比对的字段
  	  compareMap.put(0,"company_id;companyId");
  	  compareMap.put(1,"title;instrumentTitle");
  	  compareMap.put(2,"ajbh;caseNumber");
  	  compareMap.put(3,"date;date");
  	  compareMap.put(4,"plaintiff;plaintiff");
  	  compareMap.put(5,"defendant;defendant");
  	  compareMap.put(6,"content;paperContent");
  	  compareMap.put(7,"caseType;caseType");
  	  compareMap.put(8,"reason;reason");
  	  compareMap.put(9,"ajbh_md5;caseId");
  	  compareMap.put(10,"state;state");
	  compareMap.put(11,"uptime;uptime");
  	  if(compareMongoAndEs(compareMap))
  	  {
  		  System.out.println("同步更新成功,测试完成，程序正常");
  	  };
  	  
	}

	//更新Legalinstrument表的company_id
	@org.junit.jupiter.api.Test
	void testLegalinstrumentUpdateCompanyId() throws Exception
	{
	  //测试的mongo表名	
	  String collectionName="tb_wenshu";
	  Map ids=new HashMap();
	  //需要更新的key和value
	  Map newValue=new HashMap();
	  newValue.put("company_id", 100);
	  //拿到更新后的数据
	  updateToMongo(collectionName, conditionUpdate, newValue);
	  //拿到更新后的数据
	  mongoRecord=srcDataSource.query(collectionName, conditionUpdate);
	  //对wenshu的mongo数据进行必要的修饰
	  operateRecord(mongoRecord);
	  for(int i=0;i<mongoRecord.size();i++)
	  {
		  ids.put(i+1, mongoRecord.get(i).getData().get("_id"));
		  System.out.println(mongoRecord.get(i).getData().get("_id"));
	  }
	  Thread.sleep(5000);
	  getESUpdate(ids, "legalinstrument_test");
	  Map compareMap=new HashMap();
	  //设置需要比对的字段
	  compareMap.put(0,"company_id;companyId");
	  if(compareMongoAndEs(compareMap)==true)
	  {
		  System.out.println("同步更新成功");
	  }
	  else {
		  System.out.println("同步更新失败");
	  }	
	}		
	
	//更新Legalinstrument表的title
	@org.junit.jupiter.api.Test
	void testLegalinstrumentUpdateTitle() throws Exception
	{
	  //测试的mongo表名	
	  String collectionName="tb_wenshu";
	  Map ids=new HashMap();
	  //需要更新的key和value
	  Map newValue=new HashMap();
	  newValue.put("title", "黄信山与孙干年、缪小丽民间借贷纠纷一审民事裁定书");
	  //拿到更新后的数据
	  updateToMongo(collectionName, conditionUpdate, newValue);
	  //拿到更新后的数据
	  mongoRecord=srcDataSource.query(collectionName, conditionUpdate);
	  //对wenshu的mongo数据进行必要的修饰
	  operateRecord(mongoRecord);
	  for(int i=0;i<mongoRecord.size();i++)
	  {
		  ids.put(i+1, mongoRecord.get(i).getData().get("_id"));
		  System.out.println(mongoRecord.get(i).getData().get("_id"));
	  }
	  Thread.sleep(5000);
	  getESUpdate(ids, "legalinstrument_test");
	  Map compareMap=new HashMap();
	  //设置需要比对的字段
	  compareMap.put(0,"title;instrumentTitle");
	  if(compareMongoAndEs(compareMap)==true)
	  {
		  System.out.println("同步更新成功");
	  }
	  else {
		  System.out.println("同步更新失败");
	  }	
	}		
	
	//更新Legalinstrument表的ajbh
	@org.junit.jupiter.api.Test
	void testLegalinstrumentUpdateCaseNumber() throws Exception
	{
	  //测试的mongo表名	
	  String collectionName="tb_wenshu";
	  Map ids=new HashMap();
	  //需要更新的key和value
	  Map newValue=new HashMap();
	  newValue.put("ajbh", "（2018）双民初字第169号");
	  //拿到更新后的数据
	  updateToMongo(collectionName, conditionUpdate, newValue);
	  //拿到更新后的数据
	  mongoRecord=srcDataSource.query(collectionName, conditionUpdate);
	  //对wenshu的mongo数据进行必要的修饰
	  operateRecord(mongoRecord);
	  for(int i=0;i<mongoRecord.size();i++)
	  {
		  ids.put(i+1, mongoRecord.get(i).getData().get("_id"));
		  System.out.println(mongoRecord.get(i).getData().get("_id"));
	  }
	  Thread.sleep(5000);
	  getESUpdate(ids, "legalinstrument_test");
	  Map compareMap=new HashMap();
	  //设置需要比对的字段
	  compareMap.put(0,"ajbh;caseNumber");
	  if(compareMongoAndEs(compareMap)==true)
	  {
		  System.out.println("同步更新成功");
	  }
	  else {
		  System.out.println("同步更新失败");
	  }	
	}		
	
	//更新Legalinstrument表的date
	@org.junit.jupiter.api.Test
	void testLegalinstrumentUpdateDate() throws Exception
	{
	  //测试的mongo表名	
	  String collectionName="tb_wenshu";
	  Map ids=new HashMap();
	  //需要更新的key和value
	  Map newValue=new HashMap();
	  newValue.put("date", "2018-06-01");
	  //拿到更新后的数据
	  updateToMongo(collectionName, conditionUpdate, newValue);
	  //拿到更新后的数据
	  mongoRecord=srcDataSource.query(collectionName, conditionUpdate);
	  //对wenshu的mongo数据进行必要的修饰
	  operateRecord(mongoRecord);
	  for(int i=0;i<mongoRecord.size();i++)
	  {
		  ids.put(i+1, mongoRecord.get(i).getData().get("_id"));
		  System.out.println(mongoRecord.get(i).getData().get("_id"));
	  }
	  Thread.sleep(5000);
	  getESUpdate(ids, "legalinstrument_test");
	  Map compareMap=new HashMap();
	  //设置需要比对的字段
	  compareMap.put(0,"date;date");
	  if(compareMongoAndEs(compareMap)==true)
	  {
		  System.out.println("同步更新成功");
	  }
	  else {
		  System.out.println("同步更新失败");
	  }	
	}		
	
	//更新Legalinstrument表的propoments
	@org.junit.jupiter.api.Test
	void testLegalinstrumentUpdatePlaintiff() throws Exception
	{
	  //测试的mongo表名	
	  String collectionName="tb_wenshu";
	  Map ids=new HashMap();
	  //需要更新的key和value
	  Map newValue=new HashMap();
	  Map summary=new HashMap();
	  List RelateInfo=new ArrayList();
	  Map plaintiff=new HashMap();
	  plaintiff.put("key", "propoments");
	  plaintiff.put("name", "propoments-name");
	  plaintiff.put("value", "propoments-value");
	  RelateInfo.add(plaintiff);
	  summary.put("RelateInfo",RelateInfo);
	  newValue.put("summary", summary);
	  //拿到更新后的数据
	  updateToMongo(collectionName, conditionUpdate, newValue);
	  //拿到更新后的数据
	  mongoRecord=srcDataSource.query(collectionName, conditionUpdate);
	  //对wenshu的mongo数据进行必要的修饰
	  operateRecord(mongoRecord);
	  for(int i=0;i<mongoRecord.size();i++)
	  {
		  ids.put(i+1, mongoRecord.get(i).getData().get("_id"));
		  System.out.println(mongoRecord.get(i).getData().get("_id"));
	  }
	  Thread.sleep(5000);
	  getESUpdate(ids, "legalinstrument_test");
	  Map compareMap=new HashMap();
	  //设置需要比对的字段
	  compareMap.put(0,"plaintiff;plaintiff");
	  if(compareMongoAndEs(compareMap)==true)
	  {
		  System.out.println("同步更新成功");
	  }
	  else {
		  System.out.println("同步更新失败");
	  }	
	}		
	
	//更新Legalinstrument表的opponents
	@org.junit.jupiter.api.Test
	void testLegalinstrumentUpdateDefendant() throws Exception
	{
	  //测试的mongo表名	
	  String collectionName="tb_wenshu";
	  Map ids=new HashMap();
	  //需要更新的key和value
	  Map newValue=new HashMap();
	  Map summary=new HashMap();
	  List RelateInfo=new ArrayList();
	  Map plaintiff=new HashMap();
	  plaintiff.put("key", "opponents");
	  plaintiff.put("name", "opponents-name");
	  plaintiff.put("value", "opponents-value");
	  RelateInfo.add(plaintiff);
	  summary.put("RelateInfo",RelateInfo);
	  newValue.put("summary", summary);
	  //拿到更新后的数据
	  updateToMongo(collectionName, conditionUpdate, newValue);
	  //拿到更新后的数据
	  mongoRecord=srcDataSource.query(collectionName, conditionUpdate);
	  //对wenshu的mongo数据进行必要的修饰
	  operateRecord(mongoRecord);
	  for(int i=0;i<mongoRecord.size();i++)
	  {
		  ids.put(i+1, mongoRecord.get(i).getData().get("_id"));
		  System.out.println(mongoRecord.get(i).getData().get("_id"));
	  }
	  Thread.sleep(5000);
	  getESUpdate(ids, "legalinstrument_test");
	  Map compareMap=new HashMap();
	  //设置需要比对的字段
	  compareMap.put(0,"defendant;defendant");
	  if(compareMongoAndEs(compareMap)==true)
	  {
		  System.out.println("同步更新成功");
	  }
	  else {
		  System.out.println("同步更新失败");
	  }	
	}		
	
	//更新Legalinstrument表的caseType
	@org.junit.jupiter.api.Test
	void testLegalinstrumentUpdateCaseType() throws Exception
	{
	  //测试的mongo表名	
	  String collectionName="tb_wenshu";
	  Map ids=new HashMap();
	  //需要更新的key和value
	  Map newValue=new HashMap();
	  Map summary=new HashMap();
	  List RelateInfo=new ArrayList();
	  Map plaintiff=new HashMap();
	  plaintiff.put("key", "caseType");
	  plaintiff.put("name", "caseType-name");
	  plaintiff.put("value", "caseType-value");
	  RelateInfo.add(plaintiff);
	  summary.put("RelateInfo",RelateInfo);
	  newValue.put("summary", summary);
	  //拿到更新后的数据
	  updateToMongo(collectionName, conditionUpdate, newValue);
	  //拿到更新后的数据
	  mongoRecord=srcDataSource.query(collectionName, conditionUpdate);
	  //对wenshu的mongo数据进行必要的修饰
	  operateRecord(mongoRecord);
	  for(int i=0;i<mongoRecord.size();i++)
	  {
		  ids.put(i+1, mongoRecord.get(i).getData().get("_id"));
		  System.out.println(mongoRecord.get(i).getData().get("_id"));
	  }
	  Thread.sleep(5000);
	  getESUpdate(ids, "legalinstrument_test");
	  Map compareMap=new HashMap();
	  //设置需要比对的字段
	  compareMap.put(0,"caseType;caseType");
	  if(compareMongoAndEs(compareMap)==true)
	  {
		  System.out.println("同步更新成功");
	  }
	  else {
		  System.out.println("同步更新失败");
	  }	
	}		
	
	//更新Legalinstrument表的reason
	@org.junit.jupiter.api.Test
	void testLegalinstrumentUpdateReason() throws Exception
	{
	  //测试的mongo表名	
	  String collectionName="tb_wenshu";
	  Map ids=new HashMap();
	  //需要更新的key和value
	  Map newValue=new HashMap();
	  Map summary=new HashMap();
	  List RelateInfo=new ArrayList();
	  Map plaintiff=new HashMap();
	  plaintiff.put("key", "reason");
	  plaintiff.put("name", "reason-name");
	  plaintiff.put("value", "reason-value");
	  RelateInfo.add(plaintiff);
	  summary.put("RelateInfo",RelateInfo);
	  newValue.put("summary", summary);
	  //拿到更新后的数据
	  updateToMongo(collectionName, conditionUpdate, newValue);
	  //拿到更新后的数据
	  mongoRecord=srcDataSource.query(collectionName, conditionUpdate);
	  //对wenshu的mongo数据进行必要的修饰
	  operateRecord(mongoRecord);
	  for(int i=0;i<mongoRecord.size();i++)
	  {
		  ids.put(i+1, mongoRecord.get(i).getData().get("_id"));
	  }
	  Thread.sleep(5000);
	  getESUpdate(ids, "legalinstrument_test");
	  Map compareMap=new HashMap();
	  //设置需要比对的字段
	  compareMap.put(0,"reason;reason");
	  if(compareMongoAndEs(compareMap)==true)
	  {
		  System.out.println("同步更新成功");
	  }
	  else {
		  System.out.println("同步更新失败");
	  }	
	}		
	
	//更新Legalinstrument表的content
	@org.junit.jupiter.api.Test
	void testLegalinstrumentUpdatePaperContent() throws Exception
	{
	  //测试的mongo表名	
	  String collectionName="tb_wenshu";
	  Map ids=new HashMap();
	  //需要更新的key和value
	  Map newValue=new HashMap();
	  newValue.put("content", "<a type='dir' name='WBSB'></a><div style='TEXT-ALIGN: center; LINE-HEIGHT: 25pt; MARGIN: 0.5pt 0cm; FONT-FAMILY: 宋体; FONT-SIZE: 22pt;'>长春市双阳区人民法院</div><a type='dir' name='DSRXX'></a><div style='TEXT-ALIGN: center; LINE-HEIGHT: 30pt; MARGIN: 0.5pt 0cm; FONT-FAMILY: 仿宋; FONT-SIZE: 26pt;'>民 事 判 决 书</div><a type='dir' name='SSJL'></a><div style='TEXT-ALIGN: right; LINE-HEIGHT: 30pt; MARGIN: 0.5pt 0cm;  FONT-FAMILY: 仿宋;FONT-SIZE: 16pt; '>（2015）双民初字第169号</div><a type='dir' name='AJJBQK'></a><div style='LINE-HEIGHT: 25pt;TEXT-ALIGN:justify;TEXT-JUSTIFY:inter-ideograph; TEXT-INDENT: 30pt; MARGIN: 0.5pt 0cm;FONT-FAMILY: 仿宋; FONT-SIZE: 16pt;'>原告杨舒贺，女，2009年3月24日生，汉族，住长春市双阳区。</div><a type='dir' name='CPYZ'></a><div style='LINE-HEIGHT: 25pt;TEXT-ALIGN:justify;TEXT-JUSTIFY:inter-ideograph; TEXT-INDENT: 30pt; MARGIN: 0.5pt 0cm;FONT-FAMILY: 仿宋; FONT-SIZE: 16pt;'>法定代理人国玉弟，女，1978年9月4日生，汉族，商场服务员，住址同上。</div><a type='dir' name='PJJG'></a><div style='LINE-HEIGHT: 25pt;TEXT-ALIGN:justify;TEXT-JUSTIFY:inter-ideograph; TEXT-INDENT: 30pt; MARGIN: 0.5pt 0cm;FONT-FAMILY: 仿宋; FONT-SIZE: 16pt;'>委托代理人高兰生，吉林集成律师事务所律师。</div><a type='dir' name='WBWB'></a><div style='LINE-HEIGHT: 25pt;TEXT-ALIGN:justify;TEXT-JUSTIFY:inter-ideograph; TEXT-INDENT: 30pt; MARGIN: 0.5pt 0cm;FONT-FAMILY: 仿宋; FONT-SIZE: 16pt;'>被告长春市双阳区春蕾幼儿园</div><div style='LINE-HEIGHT: 25pt;TEXT-ALIGN:justify;TEXT-JUSTIFY:inter-ideograph; TEXT-INDENT: 30pt; MARGIN: 0.5pt 0cm;FONT-FAMILY: 仿宋; FONT-SIZE: 16pt;'>法定代表人李艳，该园园长。</div><div style='LINE-HEIGHT: 25pt;TEXT-ALIGN:justify;TEXT-JUSTIFY:inter-ideograph; TEXT-INDENT: 30pt; MARGIN: 0.5pt 0cm;FONT-FAMILY: 仿宋; FONT-SIZE: 16pt;'>地址长春市双阳区。</div><div style='LINE-HEIGHT: 25pt;TEXT-ALIGN:justify;TEXT-JUSTIFY:inter-ideograph; TEXT-INDENT: 30pt; MARGIN: 0.5pt 0cm;FONT-FAMILY: 仿宋; FONT-SIZE: 16pt;'>委托代理人曹景林，该园法律顾问。</div><div style='LINE-HEIGHT: 25pt;TEXT-ALIGN:justify;TEXT-JUSTIFY:inter-ideograph; TEXT-INDENT: 30pt; MARGIN: 0.5pt 0cm;FONT-FAMILY: 仿宋; FONT-SIZE: 16pt;'>委托代理人全百玲，该园法律顾问。</div><div style='LINE-HEIGHT: 25pt;TEXT-ALIGN:justify;TEXT-JUSTIFY:inter-ideograph; TEXT-INDENT: 30pt; MARGIN: 0.5pt 0cm;FONT-FAMILY: 仿宋; FONT-SIZE: 16pt;'>原告杨舒贺诉被告长春市双阳区春蕾幼儿园（以下称春蕾幼儿园）教育机构责任纠纷一案，本院受理后依法组成合议庭公开开庭进行了审理，原告法定代理人国玉弟及委托代理人高兰生、被告委托代理人曹景林、全百玲到庭参加诉讼，本案现已审理终结。</div><div style='LINE-HEIGHT: 25pt;TEXT-ALIGN:justify;TEXT-JUSTIFY:inter-ideograph; TEXT-INDENT: 30pt; MARGIN: 0.5pt 0cm;FONT-FAMILY: 仿宋; FONT-SIZE: 16pt;'>原告诉称，原告杨舒贺系学龄前儿童，国玉弟系原告母亲，原告杨舒贺在被告春蕾幼儿园就读。2014年9月19日，原告在春蕾幼儿园教室内左眼被挂钩钩伤。原告受伤后在双阳区医院治疗，有医疗票据为凭。2014年9月19日，原告在双阳区医院门诊手册记载诊断为左眼睑外伤、左眼角膜划伤，2014年11月10日复诊补充诊断为左上泪小管断裂。有医疗手册为凭。为保护原告的合法权益，特诉至法院要求1、被告赔偿原告损失共计100310.94元，其中包括医疗费1796.74元，误工费4504.00元，营养费1000.00元，交通费1260.00元，鉴定费1500.00元，残疾赔偿金44549.20元，旅店休息费30.00元，律师费5000.00元，配镜费671.00元，继续治疗费10000.00元，精神抚慰金30000.00元。</div><div style='LINE-HEIGHT: 25pt;TEXT-ALIGN:justify;TEXT-JUSTIFY:inter-ideograph; TEXT-INDENT: 30pt; MARGIN: 0.5pt 0cm;FONT-FAMILY: 仿宋; FONT-SIZE: 16pt;'>被告辩称，根据精神损害及人身损害的相关法律规定，原告在此次事故中虽受到十级伤残，但根据吉林省审判实际，其精神损害抚慰金应保护在5000.00元；继续治疗费因原告没有实际发生也没有相关司法鉴定出具的鉴定意见，故人民法院不应支持；营养费应由医嘱或鉴定部门鉴定；配镜费与原告伤情无关联性不应保护；对7月22日原告母亲误工12天证明有异议，因原告在长春检查两次不应是12天，对原告父亲的误工证明有异议，原告检查两次也不应存在8天；对旅店临时休息费30.00无异议；室内打车30.00元的票据无异议，对于4次交通费700.00元的事实存在，但原告没提供票据，望法院酌情处理，对其它交通费无异议。</div><div style='LINE-HEIGHT: 25pt;TEXT-ALIGN:justify;TEXT-JUSTIFY:inter-ideograph; TEXT-INDENT: 30pt; MARGIN: 0.5pt 0cm;FONT-FAMILY: 仿宋; FONT-SIZE: 16pt;'>经审理查明，2014年9月19日，原告杨舒贺在春蕾幼儿园教室内被挂钩钩伤左眼，当天被送往双阳区医院治疗，诊断为左眼睑外伤、左眼角膜划伤，2014年11月10日补充诊断为上泪小管断裂，2015年7月15日吉林大学第一医院复查诊断为左上泪道阻塞，原告花去医疗费1796.74元；原告诉前自行委托吉林信达司法鉴定中心出具的鉴定意见为：被鉴定人杨舒贺此次外伤已达十级伤残。原告花去鉴定费1500.00元，庭审中被告春蕾幼儿园就原告伤残等级向本庭提出重新鉴定申请，后经长春市中级人民法院委托吉林中正司法鉴定所重新鉴定，意见为：被鉴定人杨舒贺左泪小管断裂遗留溢泪构成十级伤残。原告提供误工费损失“证明”三份，对于2015年5月25日的“证明”被告表示无异议，对于另外两份“证明”的误工时间被告表示不认可，2015年8月24日的“证明”对原告父亲杨立新工资收入未作证明，仅证明误工8天；交通费中有4次打车花费700.00元但未提供票据，被告庭审中表示认可该事实，对交通费其余数额双方无异议；对于律师代理费5000.00元及旅店住宿费30.00元双方无异议；对于配镜费671.00元原告提供吉林大学第二医院服务中心配镜商店出具的收据一张；后续治疗费10000.00元及营养费1000.00元原告未提供相关医嘱或鉴定意见。</div><div style='LINE-HEIGHT: 25pt;TEXT-ALIGN:justify;TEXT-JUSTIFY:inter-ideograph; TEXT-INDENT: 30pt; MARGIN: 0.5pt 0cm;FONT-FAMILY: 仿宋; FONT-SIZE: 16pt;'>上记事实，有原、被告陈述、医疗文证、费用票据、鉴定意见书等在卷为凭，足资认定属实。</div><div style='LINE-HEIGHT: 25pt;TEXT-ALIGN:justify;TEXT-JUSTIFY:inter-ideograph; TEXT-INDENT: 30pt; MARGIN: 0.5pt 0cm;FONT-FAMILY: 仿宋; FONT-SIZE: 16pt;'>本院认为，原告杨舒贺作为无民事行为能力人在被告春蕾幼儿园受到伤害，被告未能证明其已尽到教育、管理职责，应认定被告存在过错，对于原告的人身损害应当承担赔偿责任。医疗费依照原告提供的票据保护数额为1796.74元；伤残等级依照吉林中正司法鉴定所鉴定意见为准，伤残等级为十级，残疾赔偿金标准参照《吉林省高级人民法院关于2014年度人身损害赔偿和小额诉讼案件标的额执行标准的通知》第一条第（一）项，保护数额为44549.20元；鉴定费依照原告提供的正规票据，保护数额为1500.00元；对于原告提供的2015年5月25日的“证明”中原告母亲国玉弟的2000.00元的误工费损失双方无异议，本院予以保护；被告对2015年7月22日“证明”中国玉弟的第二次误工时间12天不认可，但未提出相关证据予以推翻，因此对于原告母亲国玉弟第二次因误工而损失的工资及奖金共1304.00元予以保护；原告提供的父亲杨立新的误工证明仅笼统指出误工8天，对于工资标准及是否因误工而扣发工资均未做说明，因此对该证明不予认可；后续治疗费及营养费因无相关医嘱或鉴定意见，对该请求不予保护；配镜费票据为非正规票据，对该费用不予保护；律师代理费5000.00元及住宿费30.00元双方无异议，本院予以保护；交通费中有700.00元原告未提供票据，但被告方对该事实认可，本院予以保护，其它交通费双方无异议，对交通费保护总额为1260.00元；精神损害抚慰金结合侵权人过错程度、造成的后果及本地平均生活水平以保护10000.00元为宜。故依照《中华人民共和国侵权责任法》第六条、第十六条、第三十八之规定，判决如下：</div><div style='LINE-HEIGHT: 25pt;TEXT-ALIGN:justify;TEXT-JUSTIFY:inter-ideograph; TEXT-INDENT: 30pt; MARGIN: 0.5pt 0cm;FONT-FAMILY: 仿宋; FONT-SIZE: 16pt;'>一、被告长春市双阳区春蕾幼儿园赔偿原告杨舒贺医疗费1796.74元、残疾赔偿金44549.20元、误工费3304.00元、鉴定费1500.00元、交通费1260.00元、住宿费30.00元、律师代理费5000.00元、精神损害抚慰金10000.00元，共计67439.94元，于本判决生效后执行；</div><div style='LINE-HEIGHT: 25pt;TEXT-ALIGN:justify;TEXT-JUSTIFY:inter-ideograph; TEXT-INDENT: 30pt; MARGIN: 0.5pt 0cm;FONT-FAMILY: 仿宋; FONT-SIZE: 16pt;'>二、驳回原告杨舒贺其它诉讼请求。</div><div style='LINE-HEIGHT: 25pt;TEXT-ALIGN:justify;TEXT-JUSTIFY:inter-ideograph; TEXT-INDENT: 30pt; MARGIN: 0.5pt 0cm;FONT-FAMILY: 仿宋; FONT-SIZE: 16pt;'>如果未按本判决指定的期限履行给付金钱义务的，应当依照《中华人民共和国民事诉讼法》第二百五十三条之规定，加倍支付迟延履行期间的债务利息。</div><div style='LINE-HEIGHT: 25pt;TEXT-ALIGN:justify;TEXT-JUSTIFY:inter-ideograph; TEXT-INDENT: 30pt; MARGIN: 0.5pt 0cm;FONT-FAMILY: 仿宋; FONT-SIZE: 16pt;'>案件受理费2150.00元，由被告长春市双阳区春蕾幼儿园承担1486.00元，其余664.00元由原告自行承担，执行时间同上。</div><div style='LINE-HEIGHT: 25pt;TEXT-ALIGN:justify;TEXT-JUSTIFY:inter-ideograph; TEXT-INDENT: 30pt; MARGIN: 0.5pt 0cm;FONT-FAMILY: 仿宋; FONT-SIZE: 16pt;'>如不服本判决，可在判决书送达之日起十五日内向本院递交上诉状，并按对方当事人的人数提出副本，上诉于吉林省长春市中级人民法院。</div><div style='TEXT-ALIGN: right; LINE-HEIGHT: 25pt; MARGIN: 0.5pt 72pt 0.5pt 0cm;FONT-FAMILY: 仿宋; FONT-SIZE: 16pt;'>审　判　长　　霍慧超</div><div style='TEXT-ALIGN: right; LINE-HEIGHT: 25pt; MARGIN: 0.5pt 72pt 0.5pt 0cm;FONT-FAMILY: 仿宋; FONT-SIZE: 16pt;'>人民陪审员　　郭金峰</div><div style='TEXT-ALIGN: right; LINE-HEIGHT: 25pt; MARGIN: 0.5pt 72pt 0.5pt 0cm;FONT-FAMILY: 仿宋; FONT-SIZE: 16pt;'>人民陪审员　　张志斌</div><br/><div style='TEXT-ALIGN: right; LINE-HEIGHT: 25pt; MARGIN: 0.5pt 72pt 0.5pt 0cm;FONT-FAMILY: 仿宋; FONT-SIZE: 16pt;'>二〇一五年九月十四日</div><div style='TEXT-ALIGN: right; LINE-HEIGHT: 25pt; MARGIN: 0.5pt 72pt 0.5pt 0cm;FONT-FAMILY: 仿宋; FONT-SIZE: 16pt;'>书　记　员　　逯志英</div>");
	  //拿到更新后的数据
	  updateToMongo(collectionName, conditionUpdate, newValue);
	  //拿到更新后的数据
	  mongoRecord=srcDataSource.query(collectionName, conditionUpdate);
	  //对wenshu的mongo数据进行必要的修饰
	  operateRecord(mongoRecord);
	  for(int i=0;i<mongoRecord.size();i++)
	  {
		  ids.put(i+1, mongoRecord.get(i).getData().get("_id"));
	  }
	  Thread.sleep(5000);
	  getESUpdate(ids, "legalinstrument_test");
	  Map compareMap=new HashMap();
	  //设置需要比对的字段
	  compareMap.put(0,"content;paperContent");
	  if(compareMongoAndEs(compareMap)==true)
	  {
		  System.out.println("同步更新成功");
	  }
	  else {
		  System.out.println("同步更新失败");
	  }	
	}		
	
	//更新Legalinstrument表的ajbh_md5
	@org.junit.jupiter.api.Test
	void testLegalinstrumentUpdateCaseId() throws Exception
	{
	  //测试的mongo表名	
	  String collectionName="tb_wenshu";
	  Map ids=new HashMap();
	  //需要更新的key和value
	  Map newValue=new HashMap();
	  newValue.put("ajbh_md5", "368c3fb83fae5e6c607e57b9bdc498bf");
	  //拿到更新后的数据
	  updateToMongo(collectionName, conditionUpdate, newValue);
	  //拿到更新后的数据
	  mongoRecord=srcDataSource.query(collectionName, conditionUpdate);
	  //对wenshu的mongo数据进行必要的修饰
	  operateRecord(mongoRecord);
	  for(int i=0;i<mongoRecord.size();i++)
	  {
		  ids.put(i+1, mongoRecord.get(i).getData().get("_id"));
	  }
	  Thread.sleep(5000);
	  getESUpdate(ids, "legalinstrument_test");
	  Map compareMap=new HashMap();
	  //设置需要比对的字段
	  compareMap.put(0,"ajbh_md5;caseId");
	  if(compareMongoAndEs(compareMap)==true)
	  {
		  System.out.println("同步更新成功");
	  }
	  else {
		  System.out.println("同步更新失败");
	  }	
	}		
	
	@org.junit.jupiter.api.Test
	void testBranches() throws IOException, InterruptedException {	  
  	  String collectionName="sanban_company_branch";
  	  Map conditonal=new HashMap();
  	  conditonal.put("opType", "query");
  	  Map data=new HashMap();
  	  conditonal.put("data", data);
  	  Map filterMap = new HashMap();
  	  Map gtMap  = new HashMap();
  	  gtMap.put("$gt", new ObjectId("000000000000000000000000"));
  	  filterMap.put("_id", gtMap);
  	  Map projectionMap=new HashMap();
  	  projectionMap.put("_id", 0);
  	  data.put("filter", filterMap);
  	  data.put("limit", 600);
  	  data.put("projection", projectionMap);
  	  //查询数据并新增数据
  	  insertToMongo(collectionName, conditonal);
  	  Thread.sleep(10000);
  	  getESUpdate(ids, "branches_test");
  	  Map compareMap=new HashMap();
  	  //设置需要比对的字段
  	  compareMap.put(0,"company_id;companyId");
  	  compareMap.put(1,"relation_id;branchId");
  	  if(compareMongoAndEs(compareMap))
  	  {
  		  System.out.println("同步更新成功,测试完成，程序正常");
  	  };	  
	}
	
	//更新Branch表的company_id
	@org.junit.jupiter.api.Test
	void testBranchUpdateCompanyId() throws Exception
	{
	  //测试的mongo表名	
	  String collectionName="sanban_company_branch";
	  Map ids=new HashMap();
	  //需要更新的key和value
	  Map newValue=new HashMap();
	  newValue.put("company_id", 100);
	  //拿到更新后的数据
	  updateToMongo(collectionName, conditionUpdate, newValue);
	  //拿到更新后的数据
	  mongoRecord=srcDataSource.query(collectionName, conditionUpdate);
	  for(int i=0;i<mongoRecord.size();i++)
	  {
		  ids.put(i+1, mongoRecord.get(i).getData().get("_id"));
		  System.out.println(mongoRecord.get(i).getData().get("_id"));
	  }
	  Thread.sleep(5000);
	  getESUpdate(ids, "branches_test");
	  Map compareMap=new HashMap();
	  //设置需要比对的字段
	  compareMap.put(0,"company_id;companyId");
	  if(compareMongoAndEs(compareMap)==true)
	  {
		  System.out.println("同步更新成功");
	  }
	  else {
		  System.out.println("同步更新失败");
	  }	
	}		
	
	//更新Branch表的branch_id
	@org.junit.jupiter.api.Test
	void testBranchUpdateBranchId() throws Exception
	{
	  //测试的mongo表名	
	  String collectionName="sanban_company_branch";
	  Map ids=new HashMap();
	  //需要更新的key和value
	  Map newValue=new HashMap();
	  newValue.put("realation_id", 100);
	  //拿到更新后的数据
	  updateToMongo(collectionName, conditionUpdate, newValue);
	  //拿到更新后的数据
	  mongoRecord=srcDataSource.query(collectionName, conditionUpdate);
	  for(int i=0;i<mongoRecord.size();i++)
	  {
		  ids.put(i+1, mongoRecord.get(i).getData().get("_id"));
		  System.out.println(mongoRecord.get(i).getData().get("_id"));
	  }
	  Thread.sleep(5000);
	  getESUpdate(ids, "branches_test");
	  Map compareMap=new HashMap();
	  //设置需要比对的字段
	  compareMap.put(0,"realation_id;branchId");
	  if(compareMongoAndEs(compareMap)==true)
	  {
		  System.out.println("同步更新成功");
	  }
	  else {
		  System.out.println("同步更新失败");
	  }	
	}			
	
	//测试索引Product
	@org.junit.jupiter.api.Test
	void testProduct() throws IOException, InterruptedException {
	  
  	  String collectionName="sanban_company_product";
  	  Map conditonal=new HashMap();
  	  conditonal.put("opType", "query");
  	  Map data=new HashMap();
  	  conditonal.put("data", data);
  	  Map filterMap = new HashMap();
  	  Map gtMap  = new HashMap();
  	  gtMap.put("$gt", new ObjectId("000000000000000000000000"));
  	  filterMap.put("_id", gtMap);
  	  Map projectionMap=new HashMap();
  	  projectionMap.put("_id", 0);
  	  data.put("filter", filterMap);
  	  data.put("limit", 1000);
  	  data.put("projection", projectionMap);
  	  //查询数据并新增数据
  	  insertToMongo(collectionName, conditonal);
  	  Thread.sleep(10000);
  	  getESUpdate(ids, "products_test");
  	  Map compareMap=new HashMap();
  	  //设置需要比对的字段
  	  compareMap.put(0,"download_no;downloadNumber");
  	  compareMap.put(1,"company_id;companyId");
  	  compareMap.put(2,"update_time;updateTime");
  	  compareMap.put(3,"app_name;appName");
  	  compareMap.put(4,"score;score");
  	  compareMap.put(5,"app_info;appInfo");
  	  if(compareMongoAndEs(compareMap))
  	  {
  		  System.out.println("同步更新成功,测试完成，程序正常");
  	  };
  	  
	}
	
	//更新product表的update_time字段
	@org.junit.jupiter.api.Test
	void testProductUpdateUpdateTime() throws Exception
	{
	  //测试的mongo表名	
	  String collectionName="sanban_company_product";
	  Map ids=new HashMap();
	  //需要更新的key和value
	  Map newValue=new HashMap();
	  newValue.put("update_time", "2018-05-31");
	  //拿到更新后的数据
	  updateToMongo(collectionName, conditionUpdate, newValue);
	  //拿到更新后的数据
	  mongoRecord=srcDataSource.query(collectionName, conditionUpdate);
	  for(int i=0;i<mongoRecord.size();i++)
	  {
		  ids.put(i+1, mongoRecord.get(i).getData().get("_id"));
	  }
	  Thread.sleep(5000);
	  getESUpdate(ids, "products_test");
	  Map compareMap=new HashMap();
	  //设置需要比对的字段
	  compareMap.put(0,"update_time;updateTime");
	  if(compareMongoAndEs(compareMap)==true)
	  {
		  System.out.println("同步更新成功");
	  }
	  else {
		  System.out.println("同步更新失败");
	  }	
	}		
	
	//更新product表的app_name字段
	@org.junit.jupiter.api.Test
	void testProductUpdateAppName() throws Exception
	{
	  //测试的mongo表名	
	  String collectionName="sanban_company_product";
	  Map ids=new HashMap();
	  //需要更新的key和value
	  Map newValue=new HashMap();
	  newValue.put("app_name", "绝地求生");
	  //拿到更新后的数据
	  updateToMongo(collectionName, conditionUpdate, newValue);
	  //拿到更新后的数据
	  mongoRecord=srcDataSource.query(collectionName, conditionUpdate);
	  for(int i=0;i<mongoRecord.size();i++)
	  {
		  ids.put(i+1, mongoRecord.get(i).getData().get("_id"));
	  }
	  Thread.sleep(5000);
	  getESUpdate(ids, "products_test");
	  Map compareMap=new HashMap();
	  //设置需要比对的字段
	  compareMap.put(0,"app_name;appName");
	  if(compareMongoAndEs(compareMap)==true)
	  {
		  System.out.println("同步更新成功");
	  }
	  else {
		  System.out.println("同步更新失败");
	  }	
	}		
	
	//更新product表的download_no字段
	@org.junit.jupiter.api.Test
	void testProductUpdateDownloadNumber() throws Exception
	{
	  //测试的mongo表名	
	  String collectionName="sanban_company_product";
	  Map ids=new HashMap();
	  //需要更新的key和value
	  Map newValue=new HashMap();
	  newValue.put("download_no", 99999);
	  //拿到更新后的数据
	  updateToMongo(collectionName, conditionUpdate, newValue);
	  //拿到更新后的数据
	  mongoRecord=srcDataSource.query(collectionName, conditionUpdate);
	  for(int i=0;i<mongoRecord.size();i++)
	  {
		  ids.put(i+1, mongoRecord.get(i).getData().get("_id"));
	  }
	  Thread.sleep(5000);
	  getESUpdate(ids, "products_test");
	  Map compareMap=new HashMap();
	  //设置需要比对的字段
	  compareMap.put(0,"download_no;downloadNumber");
	  if(compareMongoAndEs(compareMap)==true)
	  {
		  System.out.println("同步更新成功");
	  }
	  else {
		  System.out.println("同步更新失败");
	  }	
	}		
	
	//更新product表的score字段
	@org.junit.jupiter.api.Test
	void testProductUpdateScore() throws Exception
	{
	  //测试的mongo表名	
	  String collectionName="sanban_company_product";
	  Map ids=new HashMap();
	  //需要更新的key和value
	  Map newValue=new HashMap();
	  newValue.put("score", 7);
	  //拿到更新后的数据
	  updateToMongo(collectionName, conditionUpdate, newValue);
	  //拿到更新后的数据
	  mongoRecord=srcDataSource.query(collectionName, conditionUpdate);
	  for(int i=0;i<mongoRecord.size();i++)
	  {
		  ids.put(i+1, mongoRecord.get(i).getData().get("_id"));
	  }
	  Thread.sleep(5000);
	  getESUpdate(ids, "products_test");
	  Map compareMap=new HashMap();
	  //设置需要比对的字段
	  compareMap.put(0,"score;score");
	  if(compareMongoAndEs(compareMap)==true)
	  {
		  System.out.println("同步更新成功");
	  }
	  else {
		  System.out.println("同步更新失败");
	  }	
	}		
	
	//更新product表的app_info字段
	@org.junit.jupiter.api.Test
	void testProductUpdateAppInfo() throws Exception
	{
	  //测试的mongo表名	
	  String collectionName="sanban_company_product";
	  Map ids=new HashMap();
	  //需要更新的key和value
	  Map newValue=new HashMap();
	  newValue.put("app_info", "作为住房和城乡建设部IC卡应用服务中心文化传媒平台，自成立以来分享过上万篇干货知识，深受业内小伙伴的认可和推崇。");
	  //拿到更新后的数据
	  updateToMongo(collectionName, conditionUpdate, newValue);
	  //拿到更新后的数据
	  mongoRecord=srcDataSource.query(collectionName, conditionUpdate);
	  for(int i=0;i<mongoRecord.size();i++)
	  {
		  ids.put(i+1, mongoRecord.get(i).getData().get("_id"));
	  }
	  Thread.sleep(5000);
	  getESUpdate(ids, "products_test");
	  Map compareMap=new HashMap();
	  //设置需要比对的字段
	  compareMap.put(0,"app_info;appInfo");
	  if(compareMongoAndEs(compareMap)==true)
	  {
		  System.out.println("同步更新成功");
	  }
	  else {
		  System.out.println("同步更新失败");
	  }	
	}		
	
	//测试索引job
	@org.junit.jupiter.api.Test
	void testJob() throws Exception {	  
  	  String collectionName="sanban_company_job";
  	  Map conditonal=new HashMap();
  	  conditonal.put("opType", "query");
  	  Map data=new HashMap();
  	  conditonal.put("data", data);
  	  Map filterMap = new HashMap();
  	  Map gtMap  = new HashMap();
  	  gtMap.put("$gt", new ObjectId("000000000000000000001000"));
  	  filterMap.put("_id", gtMap);
  	  Map projectionMap=new HashMap();
  	  projectionMap.put("_id", 0);
  	  data.put("filter", filterMap);
  	  data.put("limit", 500);
  	  data.put("projection", projectionMap);
  	  //查询数据并新增数据
  	  insertToMongo(collectionName, conditonal);
  	  Thread.sleep(10000);
  	  getESUpdate(ids, "jobs_test");
  	  Map compareMap=new HashMap();
  	  //设置需要比对的字段
  	  compareMap.put(0,"company;fullname");
  	  compareMap.put(1,"utime;utime");
  	  compareMap.put(2,"title;title");
  	  compareMap.put(3,"years;years");
  	  compareMap.put(4,"education;education");
  	  compareMap.put(5,"location;location");
  	  compareMap.put(6,"discription;description");
  	  compareMap.put(7,"source;source");
  	  compareMap.put(8,"jobtype;jobType");
  	  compareMap.put(9,"url;url");
  	  compareMap.put(10,"salary_min;minSalary");
	  compareMap.put(11,"salary;salary");
  	  if(compareMongoAndEs(compareMap))
  	  {
  		  System.out.println("同步更新成功,测试完成，程序正常");
  	  };  	  
	}
	
	//更新job表的fullname
	@org.junit.jupiter.api.Test
	void testJobsUpdateFullname() throws Exception
	{
	  //测试的mongo表名	
	  String collectionName="sanban_company_job";
	  Map ids=new HashMap();
	  //需要更新的key和value
	  Map newValue=new HashMap();
	  newValue.put("company", "江南皮革厂");
	  //拿到更新后的数据
	  updateToMongo(collectionName, conditionUpdate, newValue);
	  //拿到更新后的数据
	  mongoRecord=srcDataSource.query(collectionName, conditionUpdate);
	  for(int i=0;i<mongoRecord.size();i++)
	  {
		  ids.put(i+1, mongoRecord.get(i).getData().get("_id"));
	  }
	  Thread.sleep(5000);
	  getESUpdate(ids, "jobs_test");
	  Map compareMap=new HashMap();
	  //设置需要比对的字段
	  compareMap.put(0,"company;fullname");
	  if(compareMongoAndEs(compareMap)==true)
	  {
		  System.out.println("同步更新成功");
	  }
	  else {
		  System.out.println("同步更新失败");
	  }	
	}		

	//更新job表的title
	@org.junit.jupiter.api.Test
	void testJobsUpdateTitle() throws Exception
	{
	  //测试的mongo表名	
	  String collectionName="sanban_company_job";
	  Map ids=new HashMap();
	  //需要更新的key和value
	  Map newValue=new HashMap();
	  newValue.put("title", "江南皮革厂");
	  //拿到更新后的数据
	  updateToMongo(collectionName, conditionUpdate, newValue);
	  //拿到更新后的数据
	  mongoRecord=srcDataSource.query(collectionName, conditionUpdate);
	  for(int i=0;i<mongoRecord.size();i++)
	  {
		  ids.put(i+1, mongoRecord.get(i).getData().get("_id"));
	  }
	  Thread.sleep(5000);
	  getESUpdate(ids, "jobs_test");
	  Map compareMap=new HashMap();
	  //设置需要比对的字段
	  compareMap.put(0,"title;title");
	  if(compareMongoAndEs(compareMap)==true)
	  {
		  System.out.println("同步更新成功");
	  }
	  else {
		  System.out.println("同步更新失败");
	  }	
	}		
	
	//更新job表的education
	@org.junit.jupiter.api.Test
	void testJobsUpdateEducation() throws Exception
	{
	  //测试的mongo表名	
	  String collectionName="sanban_company_job";
	  Map ids=new HashMap();
	  //需要更新的key和value
	  Map newValue=new HashMap();
	  newValue.put("education", "硕士");
	  //拿到更新后的数据
	  updateToMongo(collectionName, conditionUpdate, newValue);
	  //拿到更新后的数据
	  mongoRecord=srcDataSource.query(collectionName, conditionUpdate);
	  for(int i=0;i<mongoRecord.size();i++)
	  {
		  ids.put(i+1, mongoRecord.get(i).getData().get("_id"));
	  }
	  Thread.sleep(5000);
	  getESUpdate(ids, "jobs_test");
	  Map compareMap=new HashMap();
	  //设置需要比对的字段
	  compareMap.put(0,"education;education");
	  if(compareMongoAndEs(compareMap)==true)
	  {
		  System.out.println("同步更新成功");
	  }
	  else {
		  System.out.println("同步更新失败");
	  }	
	}		
	
	//更新job表的location
	@org.junit.jupiter.api.Test
	void testJobsUpdateLocation() throws Exception
	{
	  //测试的mongo表名	
	  String collectionName="sanban_company_job";
	  Map ids=new HashMap();
	  //需要更新的key和value
	  Map newValue=new HashMap();
	  newValue.put("location", "异地招聘");
	  //拿到更新后的数据
	  updateToMongo(collectionName, conditionUpdate, newValue);
	  //拿到更新后的数据
	  mongoRecord=srcDataSource.query(collectionName, conditionUpdate);
	  for(int i=0;i<mongoRecord.size();i++)
	  {
		  ids.put(i+1, mongoRecord.get(i).getData().get("_id"));
	  }
	  Thread.sleep(5000);
	  getESUpdate(ids, "jobs_test");
	  Map compareMap=new HashMap();
	  //设置需要比对的字段
	  compareMap.put(0,"location;location");
	  if(compareMongoAndEs(compareMap)==true)
	  {
		  System.out.println("同步更新成功");
	  }
	  else {
		  System.out.println("同步更新失败");
	  }	
	}		
	
	//更新job表的location
	@org.junit.jupiter.api.Test
	void testJobsUpdateDescription() throws Exception
	{
	  //测试的mongo表名	
	  String collectionName="sanban_company_job";
	  Map ids=new HashMap();
	  //需要更新的key和value
	  Map newValue=new HashMap();
	  newValue.put("description", "职位诱人，福利多多");
	  //拿到更新后的数据
	  updateToMongo(collectionName, conditionUpdate, newValue);
	  //拿到更新后的数据
	  mongoRecord=srcDataSource.query(collectionName, conditionUpdate);
	  for(int i=0;i<mongoRecord.size();i++)
	  {
		  ids.put(i+1, mongoRecord.get(i).getData().get("_id"));
	  }
	  Thread.sleep(5000);
	  getESUpdate(ids, "jobs_test");
	  Map compareMap=new HashMap();
	  //设置需要比对的字段
	  compareMap.put(0,"description;description");
	  if(compareMongoAndEs(compareMap)==true)
	  {
		  System.out.println("同步更新成功");
	  }
	  else {
		  System.out.println("同步更新失败");
	  }	
	}		
	
	//更新job表的url
	@org.junit.jupiter.api.Test
	void testJobsUpdateUrl() throws Exception
	{
	  //测试的mongo表名	
	  String collectionName="sanban_company_job";
	  Map ids=new HashMap();
	  //需要更新的key和value
	  Map newValue=new HashMap();
	  newValue.put("url", "http://jobs.51job.com/guangdongsheng/79248051.html?s=0");
	  //拿到更新后的数据
	  updateToMongo(collectionName, conditionUpdate, newValue);
	  //拿到更新后的数据
	  mongoRecord=srcDataSource.query(collectionName, conditionUpdate);
	  for(int i=0;i<mongoRecord.size();i++)
	  {
		  ids.put(i+1, mongoRecord.get(i).getData().get("_id"));
	  }
	  Thread.sleep(5000);
	  getESUpdate(ids, "jobs_test");
	  Map compareMap=new HashMap();
	  //设置需要比对的字段
	  compareMap.put(0,"url;url");
	  if(compareMongoAndEs(compareMap)==true)
	  {
		  System.out.println("同步更新成功");
	  }
	  else {
		  System.out.println("同步更新失败");
	  }	
	}		
	
	//新增记录到Mongo的collectionName表
	public void insertToMongo(String collectionName,Map conditonal)
	{
		  List<Record> gtRecords=srcDataSource.query(collectionName, conditonal);
		  System.out.println(gtRecords.size());
	  	  //将last_modified_time设置为null，并添加上_id属性
	  	  for(int i=0;i<gtRecords.size();i++)
	  	  {		  
	  		  System.out.println(gtRecords.get(i).getData());
	  		  gtRecords.get(i).getData().put("last_modified_time",new BSONTimestamp());
	  		  //System.out.println(gtRecords.get(i).getData().get("company_id").getClass());
	  		  if(gtRecords.get(i).getData().get("_id")!=null)
	  		  {
	  			 gtRecords.get(i).getData().put("_id",null);
	  			 gtRecords.get(i).getData().remove("_id");
	  		  }
	  	  }
	  	  this.mongoRecord=gtRecords;
	  	  
	  	  OperationResult result=srcDataSource.insert(collectionName, gtRecords);
	  	  this.ids=result.getResultIds();
	  	  for(int i=0;i<ids.size();i++)
	  	  {
	  		  System.out.println(ids.get(i+1));
	  		  this.mongoRecord.get(i).getData().put("_id", ids.get(i+1));
	  	  }
	}
	
	//获取ES的数据
	public void getESUpdate(Map ids,String destName) throws IOException {
		CloseableHttpClient httpClient;
		String Uri=esIp+destName+"/article/";
		for(int i=0;i<ids.size();i++)
		{
			httpClient=HttpClients.createDefault();
			Uri=esIp+destName+"/article/";
			Uri=Uri+ids.get(i+1);
		
			HttpGet httpGet=new HttpGet(Uri);
			System.out.println("Escuting httpRequest "+httpGet.getRequestLine());
			ResponseHandler<String> responseHandler=new ResponseHandler<String>() {
				
				@Override
				public String handleResponse(HttpResponse response) throws IOException {
					int status =response.getStatusLine().getStatusCode();
					if(status>=200&&status<300)
					{
						HttpEntity entities=response.getEntity();
						try {
							return entities!=null?EntityUtils.toString(entities):null;
						} catch (ParseException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
						
					}else
					{
						throw new ClientProtocolException("Unexspected response status:"+status);
					}
					return null;
				}
			};
			String responseBody=httpClient.execute(httpGet,responseHandler);
			//JsonObject jsonObject=new JsonParser().parse(responseBody).getAsJsonObject();
			Map jsonObject=new Gson().fromJson(responseBody, Map.class);
			//获取ES里面的source属性
			Map source=(Map) jsonObject.get("_source");
			String sourceJsonStr=JSON.toJSONString(source);
			//System.out.println(sourceJsonStr);		
			//System.out.println(responsjseBody);
			if(jsonObject!=null)
			this.esRecord.put(ids.get(i+1), source);
			httpClient.close();
		}
	}
	
	//同步更新比对
	public boolean compareMongoAndEs(Map compareMap)
	{
		TestUtil testUtil=new TestUtil();
		System.out.println(mongoRecord.size());
		System.out.println(esRecord.size());
		for(int i=0;i<mongoRecord.size();i++)
		{
			Object id=mongoRecord.get(i).getData().get("_id");
			Map esRecordMap= (Map) esRecord.get(id);
			Map mongoMap=new HashMap();
			Map esMap=new HashMap();
			for(int j=0;j<compareMap.size();j++)
			{
				String compare[]=((String) compareMap.get(j)).split(";");
				String mongoName=compare[0];
				String esSourceName=compare[1];
				//System.out.println(mongoRecord.get(i).getData().get(mongoName));
				mongoMap.put(esSourceName, mongoRecord.get(i).getData().get(mongoName));
				esMap.put(esSourceName, esRecordMap.get(esSourceName));
			}
			//输出操作后mongo的属性值
			//String sourceJsonStrMongo=JSON.toJSONString(mongoMap);
			//System.out.println(sourceJsonStrMongo);	
			if(!testUtil.compareMap(mongoMap, esMap))
			{
				return false;
			}
		}
		return true;
	}
	
	//测试更新的API方法
	public void updateToMongo(String collectionName,Map conditional,Map newValue)
	{
		Bson filter = null;
		// 进行解析
		Map data = ((Map)conditional.get("data"));
		Object tempObj = null;
		tempObj = data.get("filter");
		filter = null != tempObj ? new Document((Map)tempObj): null;
		//System.out.println(newValue);
		//System.out.println(newValue.get("last_modified_time").getClass());
		Document updateDocument=new Document("$set",new Document(newValue));
		Map newTime=new HashMap();
		Map type=new HashMap();
		type.put("$type", "timestamp");
		newTime.put("last_modified_time", type);
		updateDocument.append("$currentDate",new Document(newTime));
		//System.out.println(new Document(newValue));
		srcDataSource.updateMany(collectionName, filter, updateDocument,true);
		
		//srcDataSource.up
	}
		
	//对于Mongo中获取到的Legalinstrument，进行修饰的方法
	public void operateRecord(List<Record> mongoRecord)
	{
		for(int i=0;i<mongoRecord.size();i++)
	  	  {
	  		  Document document= (Document) mongoRecord.get(i).getData().get("summary");
	  		  List list=(List) document.get("RelateInfo");
	  		  for(int j=0;j<list.size();j++)
	  		  {
	  			  	Document RelateInfoObject=(Document) list.get(j);
	  			  	if(RelateInfoObject.get("key").equals("propoments"))
	  			  	{
	  			  		//System.out.println(RelateInfoObject.get("value"));
	  			  		if(!(RelateInfoObject.get("value").equals("")))
	  			  		{
	  			  			mongoRecord.get(i).getData().put("plaintiff", RelateInfoObject.get("value"));
	  			  		}
	  			  		continue;
	  			  	}else if(RelateInfoObject.get("key").equals("opponents"))
	  			  	{
	  			  		//System.out.println(RelateInfoObject.get("value"));
	  			  		if(!(RelateInfoObject.get("value").equals("")))
	  			  		{
	  				  		mongoRecord.get(i).getData().put("defendant", RelateInfoObject.get("value"));
	  			  		}
				  		continue;
	  			  	}else if(RelateInfoObject.get("key").equals("caseType"))
	  			  	{
					  	//System.out.println(RelateInfoObject.get("value"));
					  	if(!(RelateInfoObject.get("value").equals("")))
					  	{
					  		mongoRecord.get(i).getData().put("caseType", RelateInfoObject.get("value"));
					  	}
				  		continue;
	  			  	}
	  			  	else if(RelateInfoObject.get("key").equals("reason"))
				  	{
					  	//System.out.println(RelateInfoObject.get("value"));
					  	if(!(RelateInfoObject.get("value").equals("")))
					  	{
					  		mongoRecord.get(i).getData().put("reason", RelateInfoObject.get("value"));
					  	}
				  		continue;
				  	}else if(RelateInfoObject.get("key").equals("trialRound"))
				  	{
				  		//System.out.println(RelateInfoObject.get("value"));
					  	if(!(RelateInfoObject.get("value").equals("")))
					  	{
					  		mongoRecord.get(i).getData().put("state", RelateInfoObject.get("value"));
					  	}
				  		continue;
				  	}else if(RelateInfoObject.get("key").equals("trialDate"))
				  	{
				  		//System.out.println(RelateInfoObject.get("value"));
					  	if(!(RelateInfoObject.get("value").equals("")))
					  	{
					  		mongoRecord.get(i).getData().put("uptime", RelateInfoObject.get("value"));
					  	}
				  		continue;
				  	}
	  		  }
	  		  System.out.println(mongoRecord.get(i).getData());
	  	  }	  
	}
	
	
}
