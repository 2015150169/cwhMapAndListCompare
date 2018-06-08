package com.szu.esu.cn;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hc.client5.http.impl.sync.CloseableHttpClient;
import org.apache.hc.client5.http.impl.sync.HttpClients;
import org.apache.hc.client5.http.methods.HttpGet;
import org.apache.hc.client5.http.protocol.ClientProtocolException;
import org.apache.hc.client5.http.sync.ResponseHandler;
import org.apache.hc.core5.http.HttpEntity;
import org.apache.hc.core5.http.HttpResponse;
import org.apache.hc.core5.http.ParseException;
import org.apache.hc.core5.http.entity.EntityUtils;
import org.bson.BsonTimestamp;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.types.BSONTimestamp;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.BeforeEach;

import com.alibaba.fastjson.JSON;
import com.cninfo.mongodb2esexportsystem.bean.Record;
import com.cninfo.mongodb2esexportsystem.db.DataSource;
import com.cninfo.mongodb2esexportsystem.db.MongodbDataSource;
import com.cninfo.mongodb2esexportsystem.db.OperationResult;
import com.google.gson.Gson;

public class TestDifferent {
	private MongodbDataSource srcDataSource=null;
	private CloseableHttpClient httpClient;
  	String srcMongoDatabaseName = "es_export_test"; // all_type_data/DevelopmentTest
  	String authMongoDatabaseName = "es_export_test";	// all_type_data/DevelopmentTest
	
	//MongoDB的ip+host地址
	private String mongoDBIp="172.26.18.109:27017";
	
	private int time=0;
	
	//MongoInsert的Record
	private List<Record> mongoRecord;
	
	//MongoInsert的Record经过计算转换后的Record
	private List<Record> targetMongoRecord;
	
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
	//用于保存更新的Mongo记录的条件ConditionUpdate(副表的更新条件)
	Map conditionRelationUpdate=null;
	
	
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
	  	initConditionalRelationUpdate();
	  	
	}
	
	public void initUpdateConditional()
	{
		conditionUpdate=new HashMap();
		conditionUpdate.put("opType", "query");
  	    Map data=new HashMap();
  	    conditionUpdate.put("data", data);
  	    Map filterMap = new HashMap();
  	    Map ltMap  = new HashMap();
  	    ltMap.put("$lt", new ObjectId("000000000000000000000101"));
  	    filterMap.put("_id", ltMap);
  	    data.put("filter", filterMap);
  	    data.put("limit", 100);
	}
	
	public void initConditionalRelationUpdate()
	{
		conditionRelationUpdate=new HashMap();
		conditionRelationUpdate.put("opType", "query");
  	    Map data=new HashMap();
  	    conditionRelationUpdate.put("data", data);
  	    Map filterMap = new HashMap();
  	    Map ltMap  = new HashMap();
  	    ltMap.put("$lt", 101);
  	    filterMap.put("company_id", ltMap);
  	    data.put("filter", filterMap);
  	    data.put("limit", 10000);
	}
	
	@org.junit.jupiter.api.Test
	void testPersonInsert() throws Exception
	{
	  String collectionName="sanban_company_person";
  	  Map conditonal=new HashMap();
  	  conditonal.put("opType", "query");
  	  Map data=new HashMap();
  	  conditonal.put("data", data);
  	  Map filterMap = new HashMap();
  	  Map gtMap  = new HashMap();
  	  gtMap.put("$gt", new ObjectId("000000000000000000000010"));
  	  filterMap.put("_id", gtMap);
  	  Map projectionMap=new HashMap();
  	  projectionMap.put("_id", 0);
  	  data.put("filter", filterMap);
  	  data.put("limit", 10);
  	  data.put("projection", projectionMap);
	  Map ids=new HashMap();
	  TestDifferentUtil testDifferentUtil=new TestDifferentUtil();
	  insertToMongo(collectionName, conditonal);
	  targetMongoRecord=testDifferentUtil.getTargetPersonMongoRecord(mongoRecord, srcDataSource);
	  //System.out.println(targetMongoRecord.get(0));
	  for(int i=0;i<targetMongoRecord.size();i++)
	  {
		  ids.put(i+1, targetMongoRecord.get(i).getData().get("suspect_id"));
	  }	
	  Thread.sleep(80000);
	  boolean toObjectId=false;
	  getESUpdate(ids, "person_test",toObjectId);
	  Map compareMap=new HashMap();
  	  //设置需要比对的字段
  	  compareMap.put(0,"minCompanyType;minCompanyType");
  	  compareMap.put(1,"companyIds;companyIds");
  	  compareMap.put(2,"schoolNames;schoolNames");
  	  compareMap.put(3,"industry5th;industry5th");
  	  compareMap.put(4,"province;province");
  	  compareMap.put(5,"personName;personName");
  	  compareMap.put(6,"namePinyin;namePinyin");
  	  compareMap.put(7,"companyNo;companyNo");
  	  compareMap.put(8,"basicInfo;basicInfo");
  	  compareMap.put(9,"educationBackground;educationBackground");
  	  compareMap.put(10,"resume;resume");
	  compareMap.put(11,"workExperience;workExperience");
	  compareMap.put(12,"relatedCompany;relatedCompany");
	  compareMap.put(13,"rsd_resume_parse;rsd_resume_parse");
	  String relationId="suspect_id";
	  if(compareMongoAndEs(compareMap,relationId)==true)
	  {
		  System.out.println("相同");
	  }
	  else {
		  System.out.println("不同");
	  }
		
	}
		
	//更新Person表(主表)的name字段
	@org.junit.jupiter.api.Test
	void testPersonUpdateName() throws Exception
	{
	  String collectionName="sanban_company_person";
  	  Map conditonal=new HashMap();
  	  conditonal.put("opType", "query");
  	  Map data=new HashMap();
  	  conditonal.put("data", data);
  	  Map filterMap = new HashMap();
  	  Map ltMap  = new HashMap();
  	  ltMap.put("$lt", new ObjectId("000000000000000000000101"));
  	  filterMap.put("_id", ltMap);
  	  data.put("filter", filterMap);
  	  data.put("limit", 100);
	  Map ids=new HashMap();
	  TestDifferentUtil testDifferentUtil=new TestDifferentUtil();
	  Map newValue=new HashMap();
	  newValue.put("name", "蔡");
	  //更新数据
	  updateToMongo(collectionName, conditonal, newValue);
	  //拿到更新后的数据
	  mongoRecord=srcDataSource.query(collectionName, conditonal);
	  targetMongoRecord=testDifferentUtil.getTargetPersonMongoRecord(mongoRecord, srcDataSource);
	  System.out.println("targetMongoRecord: "+targetMongoRecord.size());
	  //System.out.println(targetMongoRecord.get(0));
	  for(int i=0;i<targetMongoRecord.size();i++)
	  {
		  ids.put(i+1, targetMongoRecord.get(i).getData().get("suspect_id"));
		  //System.out.println(targetMongoRecord.get(i).getData().get("suspect_id"));
	  }	
	  Thread.sleep(80000);
	  boolean toObjectId=false;
	  getESUpdate(ids, "person_test",toObjectId);
	  Map compareMap=new HashMap();
  	  //设置需要比对的字段
  	  compareMap.put(0,"personName;personName");
  	  compareMap.put(1,"namePinyin;namePinyin");
  	  String relationId="suspect_id";
	  if(compareMongoAndEs(compareMap,relationId)==true)
	  {
		  System.out.println("相同");
	  }
	  else {
		  System.out.println("不同");
	  }
		
	}
	
	//更新Person表(主表)的sex字段
	@org.junit.jupiter.api.Test
	void testPersonUpdateSex() throws Exception
	{
	  String collectionName="sanban_company_person";
	  Map ids=new HashMap();
	  TestDifferentUtil testDifferentUtil=new TestDifferentUtil();
	  Map newValue=new HashMap();
	  newValue.put("sex", "女");
	  //更新数据
	  updateToMongo(collectionName, conditionUpdate, newValue);
	  //拿到更新后的数据
	  mongoRecord=srcDataSource.query(collectionName, conditionUpdate);
	  targetMongoRecord=testDifferentUtil.getTargetPersonMongoRecord(mongoRecord, srcDataSource);
	  for(int i=0;i<targetMongoRecord.size();i++)
	  {
		  ids.put(i+1, targetMongoRecord.get(i).getData().get("suspect_id"));
	  }	
	  Thread.sleep(100000);
	  boolean toObjectId=false;
	  getESUpdate(ids, "person_test",toObjectId);
	  Map compareMap=new HashMap();
  	  //设置需要比对的字段
  	  compareMap.put(0,"basicInfo;basicInfo");
  	  String relationId="suspect_id";
	  if(compareMongoAndEs(compareMap,relationId)==true)
	  {
		  System.out.println("同步更新成功");
	  }
	  else {
		  System.out.println("同步更新失败");
	  }
		
	}

	//更新Person表(主表)的birthday字段
	@org.junit.jupiter.api.Test
	void testPersonUpdateBirthday() throws Exception
	{
	  String collectionName="sanban_company_person";
	  Map ids=new HashMap();
	  TestDifferentUtil testDifferentUtil=new TestDifferentUtil();
	  //需要更新的key和value
	  Map newValue=new HashMap();
	  newValue.put("birthday", "1997");
	  //更新数据
	  updateToMongo(collectionName, conditionUpdate, newValue);
	  //拿到更新后的数据
	  mongoRecord=srcDataSource.query(collectionName, conditionUpdate);
	  targetMongoRecord=testDifferentUtil.getTargetPersonMongoRecord(mongoRecord, srcDataSource);
	  for(int i=0;i<targetMongoRecord.size();i++)
	  {
		  ids.put(i+1, targetMongoRecord.get(i).getData().get("suspect_id"));
	  }	
	  Thread.sleep(100000);
	  boolean toObjectId=false;
	  getESUpdate(ids, "person_test",toObjectId);
	  Map compareMap=new HashMap();
  	  //设置需要比对的字段
  	  compareMap.put(0,"basicInfo;basicInfo");
  	  String relationId="suspect_id";
	  if(compareMongoAndEs(compareMap,relationId)==true)
	  {
		  System.out.println("同步更新成功");
	  }
	  else {
		  System.out.println("同步更新失败");
	  }	
	}
	
	//更新Person表(主表)的education字段
	@org.junit.jupiter.api.Test
	void testPersonUpdateEducation() throws Exception
	{
	  String collectionName="sanban_company_person";
	  Map ids=new HashMap();
	  TestDifferentUtil testDifferentUtil=new TestDifferentUtil();
	  //需要更新的key和value
	  Map newValue=new HashMap();
	  newValue.put("education", "博士");
	  //更新数据
	  updateToMongo(collectionName, conditionUpdate, newValue);
	  //拿到更新后的数据
	  mongoRecord=srcDataSource.query(collectionName, conditionUpdate);
	  targetMongoRecord=testDifferentUtil.getTargetPersonMongoRecord(mongoRecord, srcDataSource);
	  for(int i=0;i<targetMongoRecord.size();i++)
	  {
		  ids.put(i+1, targetMongoRecord.get(i).getData().get("suspect_id"));
	  }	
	  Thread.sleep(100000);
	  boolean toObjectId=false;
	  getESUpdate(ids, "person_test",toObjectId);
	  Map compareMap=new HashMap();
  	  //设置需要比对的字段
  	  compareMap.put(0,"basicInfo;basicInfo");
  	  String relationId="suspect_id";
	  if(compareMongoAndEs(compareMap,relationId)==true)
	  {
		  System.out.println("同步更新成功");
	  }
	  else {
		  System.out.println("同步更新失败");
	  }	
	}
	
	
	//更新Person表(主表)的EducatExperience字段
	@org.junit.jupiter.api.Test
	void testPersonUpdateEducatExperience() throws Exception
	{
	  String collectionName="sanban_company_person";
	  Map ids=new HashMap();
	  TestDifferentUtil testDifferentUtil=new TestDifferentUtil();
	  //需要更新的key和value
	  Map newValue=new HashMap();
	  ArrayList list=new ArrayList();
	  Map first=new HashMap();
	  first.put("college", "北京邮电大学");
	  first.put("cmajor", "软件工程");
	  Map second=new HashMap<>();
	  second.put("college", "清华大学");
	  second.put("cmajor", "人工智能方向");
	  list.add(first);
	  list.add(second);
	  newValue.put("educat_experience", list);
	  newValue.put("sex", "男");
	  //更新数据
	  updateToMongo(collectionName, conditionUpdate, newValue);
	  //拿到更新后的数据
	  mongoRecord=srcDataSource.query(collectionName, conditionUpdate);
	  targetMongoRecord=testDifferentUtil.getTargetPersonMongoRecord(mongoRecord, srcDataSource);
	  for(int i=0;i<targetMongoRecord.size();i++)
	  {
		  ids.put(i+1, targetMongoRecord.get(i).getData().get("suspect_id"));
	  }	
	  Thread.sleep(100000);
	  boolean toObjectId=false;
	  getESUpdate(ids, "person_test",toObjectId);
	  Map compareMap=new HashMap();
  	  //设置需要比对的字段
  	  compareMap.put(0,"educationBackground;educationBackground");
  	  compareMap.put(1,"basicInfo;basicInfo");
  	  String relationId="suspect_id";
	  if(compareMongoAndEs(compareMap,relationId)==true)
	  {
		  System.out.println("同步更新成功");
	  }
	  else {
		  System.out.println("同步更新失败");
	  }	
	}
	
	
	//更新Person表(主表)的resume字段
	@org.junit.jupiter.api.Test
	void testPersonUpdateResume() throws Exception
	{
	  String collectionName="sanban_company_person";
	  Map ids=new HashMap();
	  TestDifferentUtil testDifferentUtil=new TestDifferentUtil();
	  //需要更新的key和value
	  Map newValue=new HashMap();
	  String resume="AAA,女,1987年2月出生,中国籍,无境外永久居留权。2009年6月,毕业于四川音乐学院,学士学位;2009年11月至2014年1月,任中视典数字科技有限公司市场部经理;2014年2月至今,任泰合鼎川物联科技(北京)股份有限公司市场部副总监;现任泰合鼎川物联科技(北京)股份有限公司监事会主席,任期三年。  \\t\\t\\t\\t\\t\\t\\t\\t\\t\\t\\t\\t\\t此简介更新于2018-03-30";
	  newValue.put("resume", resume);
	  //更新数据
	  updateToMongo(collectionName, conditionUpdate, newValue);
	  //拿到更新后的数据
	  mongoRecord=srcDataSource.query(collectionName, conditionUpdate);
	  targetMongoRecord=testDifferentUtil.getTargetPersonMongoRecord(mongoRecord, srcDataSource);
	  for(int i=0;i<targetMongoRecord.size();i++)
	  {
		  ids.put(i+1, targetMongoRecord.get(i).getData().get("suspect_id"));
	  }	
	  Thread.sleep(100000);
	  boolean toObjectId=false;
	  getESUpdate(ids, "person_test",toObjectId);
	  Map compareMap=new HashMap();
  	  //设置需要比对的字段
  	  compareMap.put(0,"resume;resume");
  	  String relationId="suspect_id";
	  if(compareMongoAndEs(compareMap,relationId)==true)
	  {
		  System.out.println("同步更新成功");
	  }
	  else {
		  System.out.println("同步更新失败");
	  }	
	}
	
	//更新Person表(主表)的rsd_resume_parse字段
	@org.junit.jupiter.api.Test
	void testPersonUpdateRsdResumeParse() throws Exception
	{
	  String collectionName="sanban_company_person";
	  Map ids=new HashMap();
	  TestDifferentUtil testDifferentUtil=new TestDifferentUtil();
	  //需要更新的key和value
	  Map newValue=new HashMap();
	  String rsdResumeParse="{\n  \"person_basic_info\": {\n    \"school\": [\n      {\n        \"major\": \"\", \n        \"shname\": \"北京邮电学院\", \n        \"time\": \"\"\n      }\n    ], \n    \"name\": \"AA\", \n    \"degree\": \"学士学位\", \n    \"sex\": \"女\", \n    \"birth\": \"1997年2月\", \n    \"nationality\": \"中国籍\"\n  }, \n  \"person_prework_info\": [\n    {\n      \"org\": \"深圳证券信息技术有限公司\", \n      \"hasorg\": true, \n      \"pos\": \"副总监\", \n      \"time\": \"2009年11月/t 至2014年1月/t \"\n    }\n  ], \n  \"person_curwork_info\": [\n    {\n      \"org\": \"阿里巴巴有限公司\", \n      \"hasorg\": true, \n      \"pos\": \"研发部副总监\", \n      \"time\": \"2014年2月/t 至今\"\n    }, \n    {\n      \"org\": \"泰合鼎川物联科技(北京)股份有限公司\", \n      \"hasorg\": true, \n      \"pos\": \"监事会主席\", \n      \"time\": \"现任\"\n    }\n  ]\n}\n";
	  newValue.put("rsd_resume_parse", rsdResumeParse);
	  updateToMongo(collectionName, conditionUpdate, newValue);
	  //拿到更新后的数据
	  mongoRecord=srcDataSource.query(collectionName, conditionUpdate);
	  targetMongoRecord=testDifferentUtil.getTargetPersonMongoRecord(mongoRecord, srcDataSource);
	  for(int i=0;i<targetMongoRecord.size();i++)
	  {
		  ids.put(i+1, targetMongoRecord.get(i).getData().get("suspect_id"));
	  }	
	  Thread.sleep(100000);
	  boolean toObjectId=false;
	  getESUpdate(ids, "person_test",toObjectId);
	  Map compareMap=new HashMap();
  	  //设置需要比对的字段
  	  compareMap.put(0,"rsdResumeParse;rsdResumeParse");
  	  compareMap.put(1,"workExperience;workExperience");
  	  String relationId="suspect_id";
	  if(compareMongoAndEs(compareMap,relationId)==true)
	  {
		  System.out.println("同步更新成功");
	  }
	  else {
		  System.out.println("同步更新失败");
	  }	
	}
	
	
	//更新Person表(主表)的company_id字段
	@org.junit.jupiter.api.Test
	void testPersonUpdateCompanyId() throws Exception
	{
	  String collectionName="sanban_company_person";
	  Map ids=new HashMap();
	  TestDifferentUtil testDifferentUtil=new TestDifferentUtil();
	  //需要更新的key和value
	  Map newValue=new HashMap();
	  newValue.put("company_id", 99);
	  updateToMongo(collectionName, conditionUpdate, newValue);
	  //拿到更新后的数据
	  mongoRecord=srcDataSource.query(collectionName, conditionUpdate);
	  targetMongoRecord=testDifferentUtil.getTargetPersonMongoRecord(mongoRecord, srcDataSource);
	  for(int i=0;i<targetMongoRecord.size();i++)
	  {
		  ids.put(i+1, targetMongoRecord.get(i).getData().get("suspect_id"));
	  }	
	  Thread.sleep(100000);
	  boolean toObjectId=false;
	  getESUpdate(ids, "person_test",toObjectId);
	  Map compareMap=new HashMap();
  	  //设置需要比对的字段
  	  compareMap.put(0,"relatedCompany;relatedCompany");
  	  compareMap.put(1,"province;province");
	  compareMap.put(2,"industry5th;industry5th");
  	  compareMap.put(3,"companyIds;companyIds");
  	  String relationId="suspect_id";
	  if(compareMongoAndEs(compareMap,relationId)==true)
	  {
		  System.out.println("同步更新成功");
	  }
	  else {
		  System.out.println("同步更新失败");
	  }	
	}
	
	//更新gs_company表(副表)的sseIndustry字段
	@org.junit.jupiter.api.Test
	void testPersonUpdateGsCompanySseIndustry() throws Exception
	{
	  String collectionNameReation="gs_company";
	  String collectionName="sanban_company_person";
	  Map ids=new HashMap();
	  TestDifferentUtil testDifferentUtil=new TestDifferentUtil();
	  //需要更新的key和value
	  Map newValue=new HashMap();
	  newValue.put("sse_industry","D04040101" );
	  updateToMongo(collectionNameReation, conditionUpdate, newValue);
	  //拿到更新后的数据
	  mongoRecord=srcDataSource.query(collectionName, conditionRelationUpdate);
	  targetMongoRecord=testDifferentUtil.getTargetPersonMongoRecord(mongoRecord, srcDataSource);
	  System.out.println(targetMongoRecord.size());
	  for(int i=0;i<targetMongoRecord.size();i++)
	  {
		  ids.put(i+1, targetMongoRecord.get(i).getData().get("suspect_id"));
	  }	
	  Thread.sleep(100000);
	  boolean toObjectId=false;
	  getESUpdate(ids, "person_test",toObjectId);
	  Map compareMap=new HashMap();
  	  //设置需要比对的字段
	  compareMap.put(0,"industry5th;industry5th");
	  String relationId="suspect_id";
	  if(compareMongoAndEs(compareMap,relationId)==true)
	  {
		  System.out.println("同步更新成功");
	  }
	  else {
		  System.out.println("同步更新失败");
	  }	
	}
	
	//更新gs_company表(副表)的fullname字段
	@org.junit.jupiter.api.Test
	void testPersonUpdateGsCompanyFullname() throws Exception
	{
	  String collectionNameReation="gs_company";
	  String collectionName="sanban_company_person";
	  Map ids=new HashMap();
	  TestDifferentUtil testDifferentUtil=new TestDifferentUtil();
	  //需要更新的key和value
	  Map newValue=new HashMap();
	  newValue.put("fullname","美国应用材料公司" );
	  updateToMongo(collectionNameReation, conditionUpdate, newValue);
	  //拿到更新后的数据
	  mongoRecord=srcDataSource.query(collectionName, conditionRelationUpdate);
	  targetMongoRecord=testDifferentUtil.getTargetPersonMongoRecord(mongoRecord, srcDataSource);
	  System.out.println(targetMongoRecord.size());
	  for(int i=0;i<targetMongoRecord.size();i++)
	  {
		  ids.put(i+1, targetMongoRecord.get(i).getData().get("suspect_id"));
	  }	
	  Thread.sleep(100000);
	  boolean toObjectId=false;
	  getESUpdate(ids, "person_test",toObjectId);
	  Map compareMap=new HashMap();
  	  //设置需要比对的字段
	  compareMap.put(0,"companyNames;companyNames");
	  String relationId="suspect_id";
	  if(compareMongoAndEs(compareMap,relationId)==true)
	  {
		  System.out.println("同步更新成功");
	  }
	  else {
		  System.out.println("同步更新失败");
	  }	
	}
	
	//更新gs_company表(副表)的province字段
	@org.junit.jupiter.api.Test
	void testPersonUpdateGsCompanyProvince() throws Exception
	{
	  String collectionNameReation="gs_company";
	  String collectionName="sanban_company_person";
	  Map ids=new HashMap();
	  TestDifferentUtil testDifferentUtil=new TestDifferentUtil();
	  //需要更新的key和value
	  Map newValue=new HashMap();
	  newValue.put("province","美国" );
	  updateToMongo(collectionNameReation, conditionUpdate, newValue);
	  //拿到更新后的数据
	  mongoRecord=srcDataSource.query(collectionName, conditionRelationUpdate);
	  targetMongoRecord=testDifferentUtil.getTargetPersonMongoRecord(mongoRecord, srcDataSource);
	  System.out.println(targetMongoRecord.size());
	  for(int i=0;i<targetMongoRecord.size();i++)
	  {
		  ids.put(i+1, targetMongoRecord.get(i).getData().get("suspect_id"));
	  }	
	  Thread.sleep(100000);
	  boolean toObjectId=false;
	  getESUpdate(ids, "person_test",toObjectId);
	  Map compareMap=new HashMap();
  	  //设置需要比对的字段
	  compareMap.put(0,"province;province");
	  String relationId="suspect_id";
	  if(compareMongoAndEs(compareMap,relationId)==true)
	  {
		  System.out.println("同步更新成功");
	  }
	  else {
		  System.out.println("同步更新失败");
	  }	
	}

	@org.junit.jupiter.api.Test
	void testSearchInsert() throws Exception
	{
		  String collectionName="gs_company";
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
	  	  data.put("limit", 500);
	  	  data.put("projection", projectionMap);
		  Map ids=new HashMap();
		  TestDifferentUtil testDifferentUtil=new TestDifferentUtil();
		  insertToMongo(collectionName, conditonal);
		  //System.out.println(mongoRecord.size());
		  targetMongoRecord=testDifferentUtil.getTargetCompanySearchMongoRecord(mongoRecord, srcDataSource);
		  //System.out.println(targetMongoRecord.get(0));
		  for(int i=0;i<targetMongoRecord.size();i++)
		  {
			  ids.put(i+1, targetMongoRecord.get(i).getData().get("company_id"));
			  System.out.println(targetMongoRecord.get(i).getData().get("company_id"));
		  }	
		  System.out.println(targetMongoRecord.size());
		  Thread.sleep(100000);
		  boolean toObjectId=true;
		  getESUpdate(ids, "search_test",toObjectId);
		  Map compareMap=new HashMap();
	  	  //设置需要比对的字段
	  	  compareMap.put(0,"latestFinancingTime;latestFinancingTime");
	  	  compareMap.put(1,"latestFinancingAmount;latestFinancingAmount");
	  	  compareMap.put(2,"latestFinancingInstitution;latestFinancingInstitution");
//	  	  compareMap.put(3,"latestFinancingInstitutionIds;latestFinancingInstitutionIds");
	  	  compareMap.put(3,"financingRound;financingRound");
	  	  compareMap.put(4,"fullname;fullname");
	  	  compareMap.put(5,"fullNamePinyin;fullNamePinyin");
	  	  compareMap.put(6,"province;province");
	  	  compareMap.put(7,"alternativeName;alternativeName");
	  	  compareMap.put(8,"alternativeNamePinyin;alternativeNamePinyin");
	  	  compareMap.put(9,"tag;tag");
		  compareMap.put(10,"sseIndustry;sseIndustry");
//		  compareMap.put(12,"sseIndustryName;sseIndustryName");
		  compareMap.put(11,"executiveName;executiveName");
		  compareMap.put(12, "executiveNamePinyin;executiveNamePinyin");
		  compareMap.put(13, "operatingStatus;operatingStatus");
		  compareMap.put(14, "registrationDate;registrationDate");
		  compareMap.put(15,"legalperson;legalperson");
		  compareMap.put(16,"registeredCapital;registeredCapital");
		  compareMap.put(17, "unifiedCode;unifiedCode");
		  compareMap.put(18, "registrationMark;registrationMark");
		  compareMap.put(19, "stockCode;stockCode");
		  compareMap.put(20,"stockSymbol;stockSymbol");
		  compareMap.put(21,"stockMarke;stockMarke");
		  compareMap.put(22, "listingDate;listingDate");
		  compareMap.put(23, "companyType;companyType");
//		  compareMap.put(26, "companyTypeCode;companyTypeCode");

		  String relationId="company_id";
		  if(compareMongoAndEs(compareMap,relationId)==true)
		  {
			  System.out.println("同步更新成功");
		  }
		  else {
			  System.out.println("不同");
		  }
	
	}
	
	//更新gs_company表(主表)的fullname字段
	@org.junit.jupiter.api.Test
	void testSearchUpdateFullname() throws Exception
	{
	  String collectionName="gs_company";
	  Map ids=new HashMap();
	  TestDifferentUtil testDifferentUtil=new TestDifferentUtil();
	  //需要更新的key和value
	  Map newValue=new HashMap();
	  newValue.put("fullname", "深圳证券信息技术有限公司");
	  updateToMongo(collectionName, conditionUpdate, newValue);
	  //拿到更新后的数据
	  mongoRecord=srcDataSource.query(collectionName, conditionUpdate);
	  targetMongoRecord=testDifferentUtil.getTargetCompanySearchMongoRecord(mongoRecord, srcDataSource);
	  for(int i=0;i<targetMongoRecord.size();i++)
	  {
		  ids.put(i+1, targetMongoRecord.get(i).getData().get("company_id"));
	  }	
	  Thread.sleep(100000);
	  boolean toObjectId=true;
	  getESUpdate(ids, "search_test",toObjectId);
	  Map compareMap=new HashMap();
  	  //设置需要比对的字段
  	  compareMap.put(0,"fullname;fullname");
  	  compareMap.put(1,"fullNamePinyin;fullNamePinyin");
  	  String relationId="company_id";
	  if(compareMongoAndEs(compareMap,relationId)==true)
	  {
		  System.out.println("同步更新成功");
	  }
	  else {
		  System.out.println("同步更新失败");
	  }	
	}
	
	//更新gs_company表(主表)的sse_industry字段
	@org.junit.jupiter.api.Test
	void testSearchUpdateSseIndustry() throws Exception
	{
	  String collectionName="gs_company";
	  Map ids=new HashMap();
	  TestDifferentUtil testDifferentUtil=new TestDifferentUtil();
	  //需要更新的key和value
	  Map newValue=new HashMap();
	  newValue.put("sse_industry", "D01010102");
	  updateToMongo(collectionName, conditionUpdate, newValue);
	  //拿到更新后的数据
	  mongoRecord=srcDataSource.query(collectionName, conditionUpdate);
	  targetMongoRecord=testDifferentUtil.getTargetCompanySearchMongoRecord(mongoRecord, srcDataSource);
	  for(int i=0;i<targetMongoRecord.size();i++)
	  {
		  ids.put(i+1, targetMongoRecord.get(i).getData().get("company_id"));
	  }	
	  Thread.sleep(100000);
	  boolean toObjectId=true;
	  getESUpdate(ids, "search_test",toObjectId);
	  Map compareMap=new HashMap();
  	  //设置需要比对的字段
  	  compareMap.put(0,"sseIndustry;sseIndustry");
  	  String relationId="company_id";
	  if(compareMongoAndEs(compareMap,relationId)==true)
	  {
		  System.out.println("同步更新成功");
	  }
	  else {
		  System.out.println("同步更新失败");
	  }	
	}
	
	//更新gs_company表(主表)的zch字段
	@org.junit.jupiter.api.Test
	void testSearchUpdateZch() throws Exception
	{
	  String collectionName="gs_company";
	  Map ids=new HashMap();
	  TestDifferentUtil testDifferentUtil=new TestDifferentUtil();
	  //需要更新的key和value
	  Map newValue=new HashMap();
	  newValue.put("zch", "440000400012565");
	  updateToMongo(collectionName, conditionUpdate, newValue);
	  //拿到更新后的数据
	  mongoRecord=srcDataSource.query(collectionName, conditionUpdate);
	  targetMongoRecord=testDifferentUtil.getTargetCompanySearchMongoRecord(mongoRecord, srcDataSource);
	  for(int i=0;i<targetMongoRecord.size();i++)
	  {
		  ids.put(i+1, targetMongoRecord.get(i).getData().get("company_id"));
	  }	
	  Thread.sleep(150000);
	  boolean toObjectId=true;
	  getESUpdate(ids, "search_test",toObjectId);
	  Map compareMap=new HashMap();
  	  //设置需要比对的字段
  	  compareMap.put(0,"registrationMark;registrationMark");
  	  String relationId="company_id";
	  if(compareMongoAndEs(compareMap,relationId)==true)
	  {
		  System.out.println("同步更新成功");
	  }
	  else {
		  System.out.println("同步更新失败");
	  }	
	}	
	
	//更新gs_company表(主表)的tym字段
	@org.junit.jupiter.api.Test
	void testSearchUpdateTym() throws Exception
	{
	  String collectionName="gs_company";
	  Map ids=new HashMap();
	  TestDifferentUtil testDifferentUtil=new TestDifferentUtil();
	  //需要更新的key和value
	  Map newValue=new HashMap();
	  newValue.put("tym", "91440300192241158P");
	  updateToMongo(collectionName, conditionUpdate, newValue);
	  //拿到更新后的数据
	  mongoRecord=srcDataSource.query(collectionName, conditionUpdate);
	  targetMongoRecord=testDifferentUtil.getTargetCompanySearchMongoRecord(mongoRecord, srcDataSource);
	  for(int i=0;i<targetMongoRecord.size();i++)
	  {
		  ids.put(i+1, targetMongoRecord.get(i).getData().get("company_id"));
	  }	
	  Thread.sleep(150000);
	  boolean toObjectId=true;
	  getESUpdate(ids, "search_test",toObjectId);
	  Map compareMap=new HashMap();
  	  //设置需要比对的字段
  	  compareMap.put(0,"unifiedCode;unifiedCode");
  	  String relationId="company_id";
	  if(compareMongoAndEs(compareMap,relationId)==true)
	  {
		  System.out.println("同步更新成功");
	  }
	  else {
		  System.out.println("同步更新失败");
	  }	
	}	
	
	//更新gs_company表(主表)的province字段
	@org.junit.jupiter.api.Test
	void testSearchUpdateProvince() throws Exception
	{
	  String collectionName="gs_company";
	  Map ids=new HashMap();
	  TestDifferentUtil testDifferentUtil=new TestDifferentUtil();
	  //需要更新的key和value
	  Map newValue=new HashMap();
	  newValue.put("province", "广东省");
	  updateToMongo(collectionName, conditionUpdate, newValue);
	  //拿到更新后的数据
	  mongoRecord=srcDataSource.query(collectionName, conditionUpdate);
	  targetMongoRecord=testDifferentUtil.getTargetCompanySearchMongoRecord(mongoRecord, srcDataSource);
	  for(int i=0;i<targetMongoRecord.size();i++)
	  {
		  ids.put(i+1, targetMongoRecord.get(i).getData().get("company_id"));
	  }	
	  Thread.sleep(150000);
	  boolean toObjectId=true;
	  getESUpdate(ids, "search_test",toObjectId);
	  Map compareMap=new HashMap();
  	  //设置需要比对的字段
  	  compareMap.put(0,"province;province");
  	  String relationId="company_id";
	  if(compareMongoAndEs(compareMap,relationId)==true)
	  {
		  System.out.println("同步更新成功");
	  }
	  else {
		  System.out.println("同步更新失败");
	  }	
	}	
		
	//更新gs_company表(主表)的othernames字段
	@org.junit.jupiter.api.Test
	void testSearchUpdateOthernames() throws Exception
	{
	  String collectionName="gs_company";
	  Map ids=new HashMap();
	  TestDifferentUtil testDifferentUtil=new TestDifferentUtil();
	  //需要更新的key和value
	  Map newValue=new HashMap();
	  ArrayList othernames=new ArrayList();
	  othernames.add("深圳证券交易所");
	  othernames.add("深圳证券信息技术有限公司");
	  newValue.put("othernames", othernames);
	  updateToMongo(collectionName, conditionUpdate, newValue);
	  //拿到更新后的数据
	  mongoRecord=srcDataSource.query(collectionName, conditionUpdate);
	  targetMongoRecord=testDifferentUtil.getTargetCompanySearchMongoRecord(mongoRecord, srcDataSource);
	  for(int i=0;i<targetMongoRecord.size();i++)
	  {
		  ids.put(i+1, targetMongoRecord.get(i).getData().get("company_id"));
	  }	
	  Thread.sleep(200000);
	  boolean toObjectId=true;
	  getESUpdate(ids, "search_test",toObjectId);
	  Map compareMap=new HashMap();
  	  //设置需要比对的字段
  	  compareMap.put(0,"alternativeName;alternativeName");
  	  compareMap.put(1,"alternativeNamePinyin;alternativeNamePinyin");
  	  String relationId="company_id";
	  if(compareMongoAndEs(compareMap,relationId)==true)
	  {
		  System.out.println("同步更新成功");
	  }
	  else {
		  System.out.println("同步更新失败");
	  }	
	}	

	
	//更新gs_company表(主表)的sse_lable字段
	@org.junit.jupiter.api.Test
	void testSearchUpdateSseLable() throws Exception
	{
	  String collectionName="gs_company";
	  Map ids=new HashMap();
	  TestDifferentUtil testDifferentUtil=new TestDifferentUtil();
	  //需要更新的key和value
	  Map newValue=new HashMap();
	  ArrayList sseLabel=new ArrayList();
	  sseLabel.add("222089");
	  sseLabel.add("222094");
	  newValue.put("sse_lable", sseLabel);
	  updateToMongo(collectionName, conditionUpdate, newValue);
	  //拿到更新后的数据
	  mongoRecord=srcDataSource.query(collectionName, conditionUpdate);
	  targetMongoRecord=testDifferentUtil.getTargetCompanySearchMongoRecord(mongoRecord, srcDataSource);
	  for(int i=0;i<targetMongoRecord.size();i++)
	  {
		  ids.put(i+1, targetMongoRecord.get(i).getData().get("company_id"));
	  }	
	  //更新时间过长
	  Thread.sleep(300000);
	  boolean toObjectId=true;
	  getESUpdate(ids, "search_test",toObjectId);
	  Map compareMap=new HashMap();
  	  //设置需要比对的字段
  	  compareMap.put(0,"tag;tag");
  	  String relationId="company_id";
	  if(compareMongoAndEs(compareMap,relationId)==true)
	  {
		  System.out.println("同步更新成功");
	  }
	  else {
		  System.out.println("同步更新失败");
	  }	
	}		
	
	//更新sanban_company_info表(副表)的clrq字段
	@org.junit.jupiter.api.Test
	void testSearchUpdateInfoClrq() throws Exception
	{
	  String collectionName="gs_company";
	  String colleactionRelationName="sanban_company_info";
	  Map ids=new HashMap();
	  TestDifferentUtil testDifferentUtil=new TestDifferentUtil();
	  //需要更新的key和value
	  Map newValue=new HashMap();
	  ArrayList sseLabel=new ArrayList();
	  newValue.put("clrq", "2018-06-01");
	  updateToMongo(colleactionRelationName, conditionRelationUpdate, newValue);
	  //拿到更新后的数据
	  mongoRecord=srcDataSource.query(collectionName, conditionUpdate);
	  targetMongoRecord=testDifferentUtil.getTargetCompanySearchMongoRecord(mongoRecord, srcDataSource);
	  for(int i=0;i<targetMongoRecord.size();i++)
	  {
		  ids.put(i+1, targetMongoRecord.get(i).getData().get("company_id"));
	  }	
	  //更新时间过长
	  Thread.sleep(300000);
	  boolean toObjectId=true;
	  getESUpdate(ids, "search_test",toObjectId);
	  Map compareMap=new HashMap();
  	  //设置需要比对的字段
  	  compareMap.put(0,"registrationDate;registrationDate");
  	  String relationId="company_id";
	  if(compareMongoAndEs(compareMap,relationId)==true)
	  {
		  System.out.println("同步更新成功");
	  }
	  else {
		  System.out.println("同步更新失败");
	  }	
	}		

	//更新sanban_company_info表(副表)的finance字段
	@org.junit.jupiter.api.Test
	void testSearchUpdateInfoFinance() throws Exception
	{
	  String collectionName="gs_company";
	  String colleactionRelationName="sanban_company_info";
	  Map ids=new HashMap();
	  TestDifferentUtil testDifferentUtil=new TestDifferentUtil();
	  //需要更新的key和value
	  Map newValue=new HashMap();
	  ArrayList sseLabel=new ArrayList();
	  newValue.put("finance", "21115.0019");
	  updateToMongo(colleactionRelationName, conditionRelationUpdate, newValue);
	  //拿到更新后的数据
	  mongoRecord=srcDataSource.query(collectionName, conditionUpdate);
	  targetMongoRecord=testDifferentUtil.getTargetCompanySearchMongoRecord(mongoRecord, srcDataSource);
	  for(int i=0;i<targetMongoRecord.size();i++)
	  {
		  ids.put(i+1, targetMongoRecord.get(i).getData().get("company_id"));
	  }	
	  //更新时间过长
	  Thread.sleep(300000);
	  boolean toObjectId=true;
	  getESUpdate(ids, "search_test",toObjectId);
	  Map compareMap=new HashMap();
  	  //设置需要比对的字段
  	  compareMap.put(0,"registeredCapital;registeredCapital");
  	  String relationId="company_id";
	  if(compareMongoAndEs(compareMap,relationId)==true)
	  {
		  System.out.println("同步更新成功");
	  }
	  else {
		  System.out.println("同步更新失败");
	  }	
	}		
	
	//更新sanban_company_info表(副表)的jyzt字段
	@org.junit.jupiter.api.Test
	void testSearchUpdateInfoOperatingStatus() throws Exception
	{
	  String collectionName="gs_company";
	  String colleactionRelationName="sanban_company_info";
	  Map ids=new HashMap();
	  TestDifferentUtil testDifferentUtil=new TestDifferentUtil();
	  //需要更新的key和value
	  Map newValue=new HashMap();
	  ArrayList sseLabel=new ArrayList();
	  newValue.put("jyzt", "存续（在营、开业、在册）");
	  updateToMongo(colleactionRelationName, conditionRelationUpdate, newValue);
	  //拿到更新后的数据
	  mongoRecord=srcDataSource.query(collectionName, conditionUpdate);
	  targetMongoRecord=testDifferentUtil.getTargetCompanySearchMongoRecord(mongoRecord, srcDataSource);
	  for(int i=0;i<targetMongoRecord.size();i++)
	  {
		  ids.put(i+1, targetMongoRecord.get(i).getData().get("company_id"));
	  }	
	  //更新时间过长
	  Thread.sleep(300000);
	  boolean toObjectId=true;
	  getESUpdate(ids, "search_test",toObjectId);
	  Map compareMap=new HashMap();
  	  //设置需要比对的字段
  	  compareMap.put(0,"operatingStatus;operatingStatus");
  	  String relationId="company_id";
	  if(compareMongoAndEs(compareMap,relationId)==true)
	  {
		  System.out.println("同步更新成功");
	  }
	  else {
		  System.out.println("同步更新失败");
	  }	
	}		
	
	//更新sanban_company_info表(副表)的legalperson字段
	@org.junit.jupiter.api.Test
	void testSearchUpdateInfoLegalperson() throws Exception
	{
	  String collectionName="gs_company";
	  String colleactionRelationName="sanban_company_info";
	  Map ids=new HashMap();
	  TestDifferentUtil testDifferentUtil=new TestDifferentUtil();
	  //需要更新的key和value
	  Map newValue=new HashMap();
	  ArrayList sseLabel=new ArrayList();
	  newValue.put("legalperson", "update杨景仁");
	  updateToMongo(colleactionRelationName, conditionRelationUpdate, newValue);
	  //拿到更新后的数据
	  mongoRecord=srcDataSource.query(collectionName, conditionUpdate);
	  targetMongoRecord=testDifferentUtil.getTargetCompanySearchMongoRecord(mongoRecord, srcDataSource);
	  for(int i=0;i<targetMongoRecord.size();i++)
	  {
		  ids.put(i+1, targetMongoRecord.get(i).getData().get("company_id"));
	  }	
	  //更新时间过长
	  Thread.sleep(300000);
	  boolean toObjectId=true;
	  getESUpdate(ids, "search_test",toObjectId);
	  Map compareMap=new HashMap();
  	  //设置需要比对的字段
  	  compareMap.put(0,"legalperson;legalperson");
  	  String relationId="company_id";
	  if(compareMongoAndEs(compareMap,relationId)==true)
	  {
		  System.out.println("同步更新成功");
	  }
	  else {
		  System.out.println("同步更新失败");
	  }	
	}		
	
	//更新sanban_company_person表(副表)的legalperson字段
	@org.junit.jupiter.api.Test
	void testSearchUpdatePersonName() throws Exception
	{
	  String collectionName="gs_company";
	  String colleactionRelationName="sanban_company_person";
	  Map ids=new HashMap();
	  TestDifferentUtil testDifferentUtil=new TestDifferentUtil();
	  //需要更新的key和value
	  Map newValue=new HashMap();
	  ArrayList sseLabel=new ArrayList();
	  newValue.put("name", "小蔡");
	  updateToMongo(colleactionRelationName, conditionRelationUpdate, newValue);
	  //拿到更新后的数据
	  mongoRecord=srcDataSource.query(collectionName, conditionUpdate);
	  targetMongoRecord=testDifferentUtil.getTargetCompanySearchMongoRecord(mongoRecord, srcDataSource);
	  for(int i=0;i<targetMongoRecord.size();i++)
	  {
		  ids.put(i+1, targetMongoRecord.get(i).getData().get("company_id"));
	  }	
	  //更新时间过长
	  Thread.sleep(300000);
	  boolean toObjectId=true;
	  getESUpdate(ids, "search_test",toObjectId);
	  Map compareMap=new HashMap();
  	  //设置需要比对的字段
  	  compareMap.put(0,"executiveName;executiveName");
  	  compareMap.put(1,"executiveNamePinyin;executiveNamePinyin");
  	  String relationId="company_id";
	  if(compareMongoAndEs(compareMap,relationId)==true)
	  {
		  System.out.println("同步更新成功");
	  }
	  else {
		  System.out.println("同步更新失败");
	  }	
	}		

	//更新tb_company表(副表)的stock_id字段
	@org.junit.jupiter.api.Test
	void testSearchUpdateTbCompanyStockId() throws Exception
	{
	  String collectionName="gs_company";
	  String colleactionRelationName="tb_company";
	  Map ids=new HashMap();
	  TestDifferentUtil testDifferentUtil=new TestDifferentUtil();
	  //需要更新的key和value
	  Map newValue=new HashMap();
	  ArrayList sseLabel=new ArrayList();
	  newValue.put("stock_id", "600973");
	  updateToMongo(colleactionRelationName, conditionRelationUpdate, newValue);
	  //拿到更新后的数据
	  mongoRecord=srcDataSource.query(collectionName, conditionUpdate);
	  targetMongoRecord=testDifferentUtil.getTargetCompanySearchMongoRecord(mongoRecord, srcDataSource);
	  for(int i=0;i<targetMongoRecord.size();i++)
	  {
		  ids.put(i+1, targetMongoRecord.get(i).getData().get("company_id"));
	  }	
	  //更新时间过长
	  Thread.sleep(300000);
	  boolean toObjectId=true;
	  getESUpdate(ids, "search_test",toObjectId);
	  Map compareMap=new HashMap();
  	  //设置需要比对的字段
  	  compareMap.put(0,"stockCode;stockCode");
  	  String relationId="company_id";
	  if(compareMongoAndEs(compareMap,relationId)==true)
	  {
		  System.out.println("同步更新成功");
	  }
	  else {
		  System.out.println("同步更新失败");
	  }	
	}		
	
	//更新tb_company表(副表)的stock_name字段
	@org.junit.jupiter.api.Test
	void testSearchUpdateTbCompanyStockName() throws Exception
	{
	  String collectionName="gs_company";
	  String colleactionRelationName="tb_company";
	  Map ids=new HashMap();
	  TestDifferentUtil testDifferentUtil=new TestDifferentUtil();
	  //需要更新的key和value
	  Map newValue=new HashMap();
	  ArrayList sseLabel=new ArrayList();
	  newValue.put("stock_name", "宝胜股份");
	  updateToMongo(colleactionRelationName, conditionRelationUpdate, newValue);
	  //拿到更新后的数据
	  mongoRecord=srcDataSource.query(collectionName, conditionUpdate);
	  targetMongoRecord=testDifferentUtil.getTargetCompanySearchMongoRecord(mongoRecord, srcDataSource);
	  for(int i=0;i<targetMongoRecord.size();i++)
	  {
		  ids.put(i+1, targetMongoRecord.get(i).getData().get("company_id"));
	  }	
	  //更新时间过长
	  Thread.sleep(300000);
	  boolean toObjectId=true;
	  getESUpdate(ids, "search_test",toObjectId);
	  Map compareMap=new HashMap();
  	  //设置需要比对的字段
  	  compareMap.put(0,"stockSymbol;stockSymbol");
  	  String relationId="company_id";
	  if(compareMongoAndEs(compareMap,relationId)==true)
	  {
		  System.out.println("同步更新成功");
	  }
	  else {
		  System.out.println("同步更新失败");
	  }	
	}		
	
	//更新tb_company表(副表)的listingDate字段
	@org.junit.jupiter.api.Test
	void testSearchUpdateTbCompanyListingDate() throws Exception
	{
	  String collectionName="gs_company";
	  String colleactionRelationName="tb_company";
	  Map ids=new HashMap();
	  TestDifferentUtil testDifferentUtil=new TestDifferentUtil();
	  //需要更新的key和value
	  Map newValue=new HashMap();
	  newValue.put("listingDate", "2018-06-01");
	  updateToMongo(colleactionRelationName, conditionRelationUpdate, newValue);
	  //拿到更新后的数据
	  mongoRecord=srcDataSource.query(collectionName, conditionUpdate);
	  targetMongoRecord=testDifferentUtil.getTargetCompanySearchMongoRecord(mongoRecord, srcDataSource);
	  for(int i=0;i<targetMongoRecord.size();i++)
	  {
		  ids.put(i+1, targetMongoRecord.get(i).getData().get("company_id"));
	  }	
	  //更新时间过长
	  Thread.sleep(300000);
	  boolean toObjectId=true;
	  getESUpdate(ids, "search_test",toObjectId);
	  Map compareMap=new HashMap();
  	  //设置需要比对的字段
  	  compareMap.put(0,"listingDate;listingDate");
  	  String relationId="company_id";
	  if(compareMongoAndEs(compareMap,relationId)==true)
	  {
		  System.out.println("同步更新成功");
	  }
	  else {
		  System.out.println("同步更新失败");
	  }	
	}		
	
	//更新investtrade表(副表)的stepnum字段
	@org.junit.jupiter.api.Test
	void testSearchUpdateInvesttradeStepnum() throws Exception
	{
	  String collectionName="gs_company";
	  String colleactionRelationName="investtrade";
	  Map ids=new HashMap();
	  TestDifferentUtil testDifferentUtil=new TestDifferentUtil();
	  //需要更新的key和value
	  Map newValue=new HashMap();
	  newValue.put("stepnum", "3980 万人民币");
	  updateToMongo(colleactionRelationName, conditionRelationUpdate, newValue);
	  //拿到更新后的数据
	  mongoRecord=srcDataSource.query(collectionName, conditionUpdate);
	  targetMongoRecord=testDifferentUtil.getTargetCompanySearchMongoRecord(mongoRecord, srcDataSource);
	  for(int i=0;i<targetMongoRecord.size();i++)
	  {
		  ids.put(i+1, targetMongoRecord.get(i).getData().get("company_id"));
	  }	
	  //更新时间过长
	  Thread.sleep(300000);
	  boolean toObjectId=true;
	  getESUpdate(ids, "search_test",toObjectId);
	  Map compareMap=new HashMap();
  	  //设置需要比对的字段
  	  compareMap.put(0,"latestFinancingAmount;latestFinancingAmount");
  	  String relationId="company_id";
	  if(compareMongoAndEs(compareMap,relationId)==true)
	  {
		  System.out.println("同步更新成功");
	  }
	  else {
		  System.out.println("同步更新失败");
	  }	
	}		
	
	//更新investtrade表(副表)的partner字段
	@org.junit.jupiter.api.Test
	void testSearchUpdateInvesttradePartner() throws Exception
	{
	  String collectionName="gs_company";
	  String colleactionRelationName="investtrade";
	  Map ids=new HashMap();
	  TestDifferentUtil testDifferentUtil=new TestDifferentUtil();
	  //需要更新的key和value
	  Map newValue=new HashMap();
	  newValue.put("partner", "名信中国成长基金");
	  updateToMongo(colleactionRelationName, conditionRelationUpdate, newValue);
	  //拿到更新后的数据
	  mongoRecord=srcDataSource.query(collectionName, conditionUpdate);
	  targetMongoRecord=testDifferentUtil.getTargetCompanySearchMongoRecord(mongoRecord, srcDataSource);
	  for(int i=0;i<targetMongoRecord.size();i++)
	  {
		  ids.put(i+1, targetMongoRecord.get(i).getData().get("company_id"));
	  }	
	  //更新时间过长
	  Thread.sleep(300000);
	  boolean toObjectId=true;
	  getESUpdate(ids, "search_test",toObjectId);
	  Map compareMap=new HashMap();
  	  //设置需要比对的字段
  	  compareMap.put(0,"latestFinancingInstitution;latestFinancingInstitution");
  	  String relationId="company_id";
	  if(compareMongoAndEs(compareMap,relationId)==true)
	  {
		  System.out.println("同步更新成功");
	  }
	  else {
		  System.out.println("同步更新失败");
	  }	
	}		
	
	//更新investtrade表(副表)的steptime字段
	@org.junit.jupiter.api.Test
	void testSearchUpdateInvesttradeSteptime() throws Exception
	{
	  String collectionName="gs_company";
	  String colleactionRelationName="investtrade";
	  Map ids=new HashMap();
	  TestDifferentUtil testDifferentUtil=new TestDifferentUtil();
	  //需要更新的key和value
	  Map newValue=new HashMap();
	  newValue.put("steptime", "2018-06-01");
	  updateToMongo(colleactionRelationName, conditionRelationUpdate, newValue);
	  //拿到更新后的数据
	  mongoRecord=srcDataSource.query(collectionName, conditionUpdate);
	  targetMongoRecord=testDifferentUtil.getTargetCompanySearchMongoRecord(mongoRecord, srcDataSource);
	  for(int i=0;i<targetMongoRecord.size();i++)
	  {
		  ids.put(i+1, targetMongoRecord.get(i).getData().get("company_id"));
	  }	
	  //更新时间过长
	  Thread.sleep(300000);
	  boolean toObjectId=true;
	  getESUpdate(ids, "search_test",toObjectId);
	  Map compareMap=new HashMap();
  	  //设置需要比对的字段
  	  compareMap.put(0,"latestFinancingTime;latestFinancingTime");
  	  String relationId="company_id";
	  if(compareMongoAndEs(compareMap,relationId)==true)
	  {
		  System.out.println("同步更新成功");
	  }
	  else {
		  System.out.println("同步更新失败");
	  }	
	}		
	
	//更新investtrade表(副表)的step字段
	@org.junit.jupiter.api.Test
	void testSearchUpdateInvesttradeStep() throws Exception
	{
	  String collectionName="gs_company";
	  String colleactionRelationName="investtrade";
	  Map ids=new HashMap();
	  TestDifferentUtil testDifferentUtil=new TestDifferentUtil();
	  //需要更新的key和value
	  Map newValue=new HashMap();
	  newValue.put("step", "A轮");
	  updateToMongo(colleactionRelationName, conditionRelationUpdate, newValue);
	  //拿到更新后的数据
	  mongoRecord=srcDataSource.query(collectionName, conditionUpdate);
	  targetMongoRecord=testDifferentUtil.getTargetCompanySearchMongoRecord(mongoRecord, srcDataSource);
	  for(int i=0;i<targetMongoRecord.size();i++)
	  {
		  ids.put(i+1, targetMongoRecord.get(i).getData().get("company_id"));
	  }	
	  //更新时间过长
	  Thread.sleep(300000);
	  boolean toObjectId=true;
	  getESUpdate(ids, "search_test",toObjectId);
	  Map compareMap=new HashMap();
  	  //设置需要比对的字段
  	  compareMap.put(0,"financingRound;financingRound");
  	  String relationId="company_id";
	  if(compareMongoAndEs(compareMap,relationId)==true)
	  {
		  System.out.println("同步更新成功");
	  }
	  else {
		  System.out.println("同步更新失败");
	  }	
	}		
	
	//更新investtrade表(副表)的investor_id字段
	@org.junit.jupiter.api.Test
	void testSearchUpdateInvesttradeInvestorId() throws Exception
	{
	  String collectionName="gs_company";
	  String colleactionRelationName="investtrade";
	  Map ids=new HashMap();
	  TestDifferentUtil testDifferentUtil=new TestDifferentUtil();
	  //需要更新的key和value
	  Map newValue=new HashMap();
	  ArrayList investorIds=new ArrayList();
	  investorIds.add(99832);
	  investorIds.add(123485);
	  newValue.put("investor_id", investorIds);
	  updateToMongo(colleactionRelationName, conditionRelationUpdate, newValue);
	  //拿到更新后的数据
	  mongoRecord=srcDataSource.query(collectionName, conditionUpdate);
	  targetMongoRecord=testDifferentUtil.getTargetCompanySearchMongoRecord(mongoRecord, srcDataSource);
	  for(int i=0;i<targetMongoRecord.size();i++)
	  {
		  ids.put(i+1, targetMongoRecord.get(i).getData().get("company_id"));
	  }	
	  //更新时间过长
	  Thread.sleep(300000);
	  boolean toObjectId=true;
	  getESUpdate(ids, "search_test",toObjectId);
	  Map compareMap=new HashMap();
  	  //设置需要比对的字段
  	  compareMap.put(0,"latestFinancingInstitutionIds;latestFinancingInstitutionIds");
  	  String relationId="company_id";
	  if(compareMongoAndEs(compareMap,relationId)==true)
	  {
		  System.out.println("同步更新成功");
	  }
	  else {
		  System.out.println("同步更新失败");
	  }	
	}		
	
	//测试basicInfo索引Insert
	@org.junit.jupiter.api.Test
	void testBasicInfoInsert() throws Exception
	{
		  String collectionName="gs_company";
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
	  	  data.put("limit", 500);
	  	  data.put("projection", projectionMap);
		  Map ids=new HashMap();
		  TestDifferentUtil testDifferentUtil=new TestDifferentUtil();
		  insertToMongo(collectionName, conditonal);
		  //System.out.println(mongoRecord.size());
		  targetMongoRecord=testDifferentUtil.getTargetCompanyStoreMongoRecord(mongoRecord, srcDataSource);
		  //System.out.println(targetMongoRecord.get(0));
		  for(int i=0;i<targetMongoRecord.size();i++)
		  {
			  ids.put(i+1, targetMongoRecord.get(i).getData().get("company_id"));
			  System.out.println(targetMongoRecord.get(i).getData().get("company_id"));
		  }	
		  System.out.println(targetMongoRecord.size());
		  Thread.sleep(100000);
		  boolean toObjectId=true;
		  getESUpdate(ids, "basicInfo_test",toObjectId);
		  Map compareMap=new HashMap();
	  	  //设置需要比对的字段
	  	  compareMap.put(0,"fullname;fullname");
	  	  compareMap.put(1,"basicInfo;basicInfo");
	  	  compareMap.put(2,"registrationInfo;registrationInfo");
	  	  compareMap.put(3,"legalpersonInfo;legalpersonInfo");
	  	  compareMap.put(4,"excutive;excutive");
	  	  compareMap.put(5,"supplierAndCustomer;supplierAndCustomer");
	  	  compareMap.put(6,"competitionAnalysis;competitionAnalysis");
		  String relationId="company_id";
		  if(compareMongoAndEs(compareMap,relationId)==true)
		  {
			  System.out.println("同步更新成功");
		  }
		  else {
			  System.out.println("不同");
		  }
	
	}

	//更新gs_company表(主表)的fullname字段
	@org.junit.jupiter.api.Test
	void testBasicInfoUpdateFullname() throws Exception
	{
	  String collectionName="gs_company";
	  Map ids=new HashMap();
	  TestDifferentUtil testDifferentUtil=new TestDifferentUtil();
	  //需要更新的key和value
	  Map newValue=new HashMap();
	  newValue.put("fullname", "深圳证券信息技术有限公司");
	  updateToMongo(collectionName, conditionUpdate, newValue);
	  //拿到更新后的数据
	  mongoRecord=srcDataSource.query(collectionName, conditionUpdate);
	  targetMongoRecord=testDifferentUtil.getTargetCompanyStoreMongoRecord(mongoRecord, srcDataSource);
	  for(int i=0;i<targetMongoRecord.size();i++)
	  {
		  ids.put(i+1, targetMongoRecord.get(i).getData().get("company_id"));
	  }	
	  Thread.sleep(100000);
	  boolean toObjectId=true;
	  getESUpdate(ids, "basicInfo_test",toObjectId);
	  Map compareMap=new HashMap();
  	  //设置需要比对的字段
  	  compareMap.put(0,"fullname;fullname");
  	  compareMap.put(1,"basicInfo;basicInfo");
  	  String relationId="company_id";
	  if(compareMongoAndEs(compareMap,relationId)==true)
	  {
		  System.out.println("同步更新成功");
	  }
	  else {
		  System.out.println("同步更新失败");
	  }	
	}

	//更新gs_company表(主表)的sse_industry字段
	@org.junit.jupiter.api.Test
	void testBasicInfoUpdateSseIndustry() throws Exception
	{
	  String collectionName="gs_company";
	  Map ids=new HashMap();
	  TestDifferentUtil testDifferentUtil=new TestDifferentUtil();
	  //需要更新的key和value
	  Map newValue=new HashMap();
	  newValue.put("sse_industry", "D01010102");
	  updateToMongo(collectionName, conditionUpdate, newValue);
	  //拿到更新后的数据
	  mongoRecord=srcDataSource.query(collectionName, conditionUpdate);
	  targetMongoRecord=testDifferentUtil.getTargetCompanyStoreMongoRecord(mongoRecord, srcDataSource);
	  for(int i=0;i<targetMongoRecord.size();i++)
	  {
		  ids.put(i+1, targetMongoRecord.get(i).getData().get("company_id"));
	  }	
	  Thread.sleep(100000);
	  boolean toObjectId=true;
	  getESUpdate(ids, "basicInfo_test",toObjectId);
	  Map compareMap=new HashMap();
  	  //设置需要比对的字段
  	  compareMap.put(0,"basicInfo;basicInfo");
  	  String relationId="company_id";
	  if(compareMongoAndEs(compareMap,relationId)==true)
	  {
		  System.out.println("同步更新成功");
	  }
	  else {
		  System.out.println("同步更新失败");
	  }	
	}
	
	//更新gs_company表(主表)的tym字段
	@org.junit.jupiter.api.Test
	void testBasicInfoUpdateTym() throws Exception
	{
	  String collectionName="gs_company";
	  Map ids=new HashMap();
	  TestDifferentUtil testDifferentUtil=new TestDifferentUtil();
	  //需要更新的key和value
	  Map newValue=new HashMap();
	  newValue.put("tym", "91440300192241158P");
	  updateToMongo(collectionName, conditionUpdate, newValue);
	  //拿到更新后的数据
	  mongoRecord=srcDataSource.query(collectionName, conditionUpdate);
	  targetMongoRecord=testDifferentUtil.getTargetCompanyStoreMongoRecord(mongoRecord, srcDataSource);
	  for(int i=0;i<targetMongoRecord.size();i++)
	  {
		  ids.put(i+1, targetMongoRecord.get(i).getData().get("company_id"));
	  }	
	  Thread.sleep(150000);
	  boolean toObjectId=true;
	  getESUpdate(ids, "basicInfo_test",toObjectId);
	  Map compareMap=new HashMap();
  	  //设置需要比对的字段
  	  compareMap.put(0,"basicInfo;basicInfo");
  	  String relationId="company_id";
	  if(compareMongoAndEs(compareMap,relationId)==true)
	  {
		  System.out.println("同步更新成功");
	  }
	  else {
		  System.out.println("同步更新失败");
	  }	
	}	
	
	//更新gs_company表(主表)的zch字段
	@org.junit.jupiter.api.Test
	void testBasicInfoUpdateZch() throws Exception
	{
	  String collectionName="gs_company";
	  Map ids=new HashMap();
	  TestDifferentUtil testDifferentUtil=new TestDifferentUtil();
	  //需要更新的key和value
	  Map newValue=new HashMap();
	  newValue.put("zch", "440000400012565");
	  updateToMongo(collectionName, conditionUpdate, newValue);
	  //拿到更新后的数据
	  mongoRecord=srcDataSource.query(collectionName, conditionUpdate);
	  targetMongoRecord=testDifferentUtil.getTargetCompanyStoreMongoRecord(mongoRecord, srcDataSource);
	  for(int i=0;i<targetMongoRecord.size();i++)
	  {
		  ids.put(i+1, targetMongoRecord.get(i).getData().get("company_id"));
	  }	
	  Thread.sleep(150000);
	  boolean toObjectId=true;
	  getESUpdate(ids, "basicInfo_test",toObjectId);
	  Map compareMap=new HashMap();
  	  //设置需要比对的字段
  	  compareMap.put(0,"registrationInfo;registrationInfo");
  	  String relationId="company_id";
	  if(compareMongoAndEs(compareMap,relationId)==true)
	  {
		  System.out.println("同步更新成功");
	  }
	  else {
		  System.out.println("同步更新失败");
	  }	
	}	
	
	//更新sanban_company_info表(副表)的address字段
	@org.junit.jupiter.api.Test
	void testBasicInfoUpdateInfoAddress() throws Exception
	{
	  String collectionName="gs_company";
	  String colleactionRelationName="sanban_company_info";
	  Map ids=new HashMap();
	  TestDifferentUtil testDifferentUtil=new TestDifferentUtil();
	  //需要更新的key和value
	  Map newValue=new HashMap();
	  ArrayList sseLabel=new ArrayList();
	  newValue.put("address", "青岛平度市新河生态化工科技产业基地丰水路5号");
	  updateToMongo(colleactionRelationName, conditionRelationUpdate, newValue);
	  //拿到更新后的数据
	  mongoRecord=srcDataSource.query(collectionName, conditionUpdate);
	  targetMongoRecord=testDifferentUtil.getTargetCompanyStoreMongoRecord(mongoRecord, srcDataSource);
	  for(int i=0;i<targetMongoRecord.size();i++)
	  {
		  ids.put(i+1, targetMongoRecord.get(i).getData().get("company_id"));
	  }	
	  //更新时间过长
	  Thread.sleep(300000);
	  boolean toObjectId=true;
	  getESUpdate(ids, "basicInfo_test",toObjectId);
	  Map compareMap=new HashMap();
  	  //设置需要比对的字段
  	  compareMap.put(0,"basicInfo;basicInfo");
  	  String relationId="company_id";
	  if(compareMongoAndEs(compareMap,relationId)==true)
	  {
		  System.out.println("同步更新成功");
	  }
	  else {
		  System.out.println("同步更新失败");
	  }	
	}		
	
	//更新sanban_company_info表(副表)的business字段
	@org.junit.jupiter.api.Test
	void testBasicInfoUpdateInfoBusiness() throws Exception
	{
	  String collectionName="gs_company";
	  String colleactionRelationName="sanban_company_info";
	  Map ids=new HashMap();
	  TestDifferentUtil testDifferentUtil=new TestDifferentUtil();
	  //需要更新的key和value
	  Map newValue=new HashMap();
	  ArrayList sseLabel=new ArrayList();
	  newValue.put("business", "氰酸钠、2，4-二氨基苯磺酸钠、磺化对位酯、间氨基乙酰苯胺、1-氨基奈-4-磺酸钠、七水硫酸镁、脱水物、2-氨基-3.6.8-萘三磺酸、轻质...");
	  updateToMongo(colleactionRelationName, conditionRelationUpdate, newValue);
	  //拿到更新后的数据
	  mongoRecord=srcDataSource.query(collectionName, conditionUpdate);
	  targetMongoRecord=testDifferentUtil.getTargetCompanyStoreMongoRecord(mongoRecord, srcDataSource);
	  for(int i=0;i<targetMongoRecord.size();i++)
	  {
		  ids.put(i+1, targetMongoRecord.get(i).getData().get("company_id"));
	  }	
	  //更新时间过长
	  Thread.sleep(300000);
	  boolean toObjectId=true;
	  getESUpdate(ids, "basicInfo_test",toObjectId);
	  Map compareMap=new HashMap();
  	  //设置需要比对的字段
  	  compareMap.put(0,"basicInfo;basicInfo");
  	  String relationId="company_id";
	  if(compareMongoAndEs(compareMap,relationId)==true)
	  {
		  System.out.println("同步更新成功");
	  }
	  else {
		  System.out.println("同步更新失败");
	  }	
	}		
	
	//更新sanban_company_info表(副表)的company_homepage字段
	@org.junit.jupiter.api.Test
	void testBasicInfoUpdateInfoCompanyHomepage() throws Exception
	{
	  String collectionName="gs_company";
	  String colleactionRelationName="sanban_company_info";
	  Map ids=new HashMap();
	  TestDifferentUtil testDifferentUtil=new TestDifferentUtil();
	  //需要更新的key和value
	  Map newValue=new HashMap();
	  ArrayList sseLabel=new ArrayList();
	  newValue.put("company_homepage", "http://www.agkhg.com");
	  updateToMongo(colleactionRelationName, conditionRelationUpdate, newValue);
	  //拿到更新后的数据
	  mongoRecord=srcDataSource.query(collectionName, conditionUpdate);
	  targetMongoRecord=testDifferentUtil.getTargetCompanyStoreMongoRecord(mongoRecord, srcDataSource);
	  for(int i=0;i<targetMongoRecord.size();i++)
	  {
		  ids.put(i+1, targetMongoRecord.get(i).getData().get("company_id"));
	  }	
	  //更新时间过长
	  Thread.sleep(300000);
	  boolean toObjectId=true;
	  getESUpdate(ids, "basicInfo_test",toObjectId);
	  Map compareMap=new HashMap();
  	  //设置需要比对的字段
  	  compareMap.put(0,"basicInfo;basicInfo");
  	  String relationId="company_id";
	  if(compareMongoAndEs(compareMap,relationId)==true)
	  {
		  System.out.println("同步更新成功");
	  }
	  else {
		  System.out.println("同步更新失败");
	  }	
	}		

	//更新sanban_company_info表(副表)的email字段
	@org.junit.jupiter.api.Test
	void testBasicInfoUpdateInfoEmail() throws Exception
	{
	  String collectionName="gs_company";
	  String colleactionRelationName="sanban_company_info";
	  Map ids=new HashMap();
	  TestDifferentUtil testDifferentUtil=new TestDifferentUtil();
	  //需要更新的key和value
	  Map newValue=new HashMap();
	  ArrayList sseLabel=new ArrayList();
	  newValue.put("email", "jinanliuwu@163.com");
	  updateToMongo(colleactionRelationName, conditionRelationUpdate, newValue);
	  //拿到更新后的数据
	  mongoRecord=srcDataSource.query(collectionName, conditionUpdate);
	  targetMongoRecord=testDifferentUtil.getTargetCompanyStoreMongoRecord(mongoRecord, srcDataSource);
	  for(int i=0;i<targetMongoRecord.size();i++)
	  {
		  ids.put(i+1, targetMongoRecord.get(i).getData().get("company_id"));
	  }	
	  //更新时间过长
	  Thread.sleep(300000);
	  boolean toObjectId=true;
	  getESUpdate(ids, "basicInfo_test",toObjectId);
	  Map compareMap=new HashMap();
  	  //设置需要比对的字段
  	  compareMap.put(0,"basicInfo;basicInfo");
  	  String relationId="company_id";
	  if(compareMongoAndEs(compareMap,relationId)==true)
	  {
		  System.out.println("同步更新成功");
	  }
	  else {
		  System.out.println("同步更新失败");
	  }	
	}		
	
	//更新sanban_company_info表(副表)的phone字段
	@org.junit.jupiter.api.Test
	void testBasicInfoUpdateInfoPhone() throws Exception
	{
	  String collectionName="gs_company";
	  String colleactionRelationName="sanban_company_info";
	  Map ids=new HashMap();
	  TestDifferentUtil testDifferentUtil=new TestDifferentUtil();
	  //需要更新的key和value
	  Map newValue=new HashMap();
	  ArrayList sseLabel=new ArrayList();
	  newValue.put("phone", "0532-86355916");
	  updateToMongo(colleactionRelationName, conditionRelationUpdate, newValue);
	  //拿到更新后的数据
	  mongoRecord=srcDataSource.query(collectionName, conditionUpdate);
	  targetMongoRecord=testDifferentUtil.getTargetCompanyStoreMongoRecord(mongoRecord, srcDataSource);
	  for(int i=0;i<targetMongoRecord.size();i++)
	  {
		  ids.put(i+1, targetMongoRecord.get(i).getData().get("company_id"));
	  }	
	  //更新时间过长
	  Thread.sleep(300000);
	  boolean toObjectId=true;
	  getESUpdate(ids, "basicInfo_test",toObjectId);
	  Map compareMap=new HashMap();
  	  //设置需要比对的字段
  	  compareMap.put(0,"basicInfo;basicInfo");
  	  String relationId="company_id";
	  if(compareMongoAndEs(compareMap,relationId)==true)
	  {
		  System.out.println("同步更新成功");
	  }
	  else {
		  System.out.println("同步更新失败");
	  }	
	}		
	
	//更新sanban_company_info表(副表)的clrp字段
	@org.junit.jupiter.api.Test
	void testBasicInfoUpdateInfoClrp() throws Exception
	{
	  String collectionName="gs_company";
	  String colleactionRelationName="sanban_company_info";
	  Map ids=new HashMap();
	  TestDifferentUtil testDifferentUtil=new TestDifferentUtil();
	  //需要更新的key和value
	  Map newValue=new HashMap();
	  ArrayList sseLabel=new ArrayList();
	  newValue.put("clrp", "2018-06-01");
	  updateToMongo(colleactionRelationName, conditionRelationUpdate, newValue);
	  //拿到更新后的数据
	  mongoRecord=srcDataSource.query(collectionName, conditionUpdate);
	  targetMongoRecord=testDifferentUtil.getTargetCompanyStoreMongoRecord(mongoRecord, srcDataSource);
	  for(int i=0;i<targetMongoRecord.size();i++)
	  {
		  ids.put(i+1, targetMongoRecord.get(i).getData().get("company_id"));
	  }	
	  //更新时间过长
	  Thread.sleep(300000);
	  boolean toObjectId=true;
	  getESUpdate(ids, "basicInfo_test",toObjectId);
	  Map compareMap=new HashMap();
  	  //设置需要比对的字段
  	  compareMap.put(0,"registrationInfo;registrationInfo");
  	  String relationId="company_id";
	  if(compareMongoAndEs(compareMap,relationId)==true)
	  {
		  System.out.println("同步更新成功");
	  }
	  else {
		  System.out.println("同步更新失败");
	  }	
	}		
	
	//更新sanban_company_info表(副表)的finance字段
	@org.junit.jupiter.api.Test
	void testBasicInfoUpdateInfoFinance() throws Exception
	{
	  String collectionName="gs_company";
	  String colleactionRelationName="sanban_company_info";
	  Map ids=new HashMap();
	  TestDifferentUtil testDifferentUtil=new TestDifferentUtil();
	  //需要更新的key和value
	  Map newValue=new HashMap();
	  ArrayList sseLabel=new ArrayList();
	  newValue.put("finance", "9500");
	  updateToMongo(colleactionRelationName, conditionRelationUpdate, newValue);
	  //拿到更新后的数据
	  mongoRecord=srcDataSource.query(collectionName, conditionUpdate);
	  targetMongoRecord=testDifferentUtil.getTargetCompanyStoreMongoRecord(mongoRecord, srcDataSource);
	  for(int i=0;i<targetMongoRecord.size();i++)
	  {
		  ids.put(i+1, targetMongoRecord.get(i).getData().get("company_id"));
	  }	
	  //更新时间过长
	  Thread.sleep(300000);
	  boolean toObjectId=true;
	  getESUpdate(ids, "basicInfo_test",toObjectId);
	  Map compareMap=new HashMap();
  	  //设置需要比对的字段
  	  compareMap.put(0,"registrationInfo;registrationInfo");
  	  String relationId="company_id";
	  if(compareMongoAndEs(compareMap,relationId)==true)
	  {
		  System.out.println("同步更新成功");
	  }
	  else {
		  System.out.println("同步更新失败");
	  }	
	}		
	
	//更新sanban_company_info表(副表)的jyzt字段
	@org.junit.jupiter.api.Test
	void testBasicInfoUpdateInfoJyzt() throws Exception
	{
	  String collectionName="gs_company";
	  String colleactionRelationName="sanban_company_info";
	  Map ids=new HashMap();
	  TestDifferentUtil testDifferentUtil=new TestDifferentUtil();
	  //需要更新的key和value
	  Map newValue=new HashMap();
	  ArrayList sseLabel=new ArrayList();
	  newValue.put("jyzt", "在营");
	  updateToMongo(colleactionRelationName, conditionRelationUpdate, newValue);
	  //拿到更新后的数据
	  mongoRecord=srcDataSource.query(collectionName, conditionUpdate);
	  targetMongoRecord=testDifferentUtil.getTargetCompanyStoreMongoRecord(mongoRecord, srcDataSource);
	  for(int i=0;i<targetMongoRecord.size();i++)
	  {
		  ids.put(i+1, targetMongoRecord.get(i).getData().get("company_id"));
	  }	
	  //更新时间过长
	  Thread.sleep(300000);
	  boolean toObjectId=true;
	  getESUpdate(ids, "basicInfo_test",toObjectId);
	  Map compareMap=new HashMap();
  	  //设置需要比对的字段
  	  compareMap.put(0,"registrationInfo;registrationInfo");
  	  String relationId="company_id";
	  if(compareMongoAndEs(compareMap,relationId)==true)
	  {
		  System.out.println("同步更新成功");
	  }
	  else {
		  System.out.println("同步更新失败");
	  }	
	}		
	
	//更新sanban_company_info表(副表)的legalperson字段
	@org.junit.jupiter.api.Test
	void testBasicInfoUpdateInfoLegalperson() throws Exception
	{
	  String collectionName="gs_company";
	  String colleactionRelationName="sanban_company_info";
	  Map ids=new HashMap();
	  TestDifferentUtil testDifferentUtil=new TestDifferentUtil();
	  //需要更新的key和value
	  Map newValue=new HashMap();
	  ArrayList sseLabel=new ArrayList();
	  newValue.put("legalperson", "李厚霖");
	  updateToMongo(colleactionRelationName, conditionRelationUpdate, newValue);
	  //拿到更新后的数据
	  mongoRecord=srcDataSource.query(collectionName, conditionUpdate);
	  targetMongoRecord=testDifferentUtil.getTargetCompanyStoreMongoRecord(mongoRecord, srcDataSource);
	  for(int i=0;i<targetMongoRecord.size();i++)
	  {
		  ids.put(i+1, targetMongoRecord.get(i).getData().get("company_id"));
	  }	
	  //更新时间过长
	  Thread.sleep(300000);
	  boolean toObjectId=true;
	  getESUpdate(ids, "basicInfo_test",toObjectId);
	  Map compareMap=new HashMap();
  	  //设置需要比对的字段
  	  compareMap.put(0,"legalpersonInfo;legalpersonInfo");
  	  String relationId="company_id";
	  if(compareMongoAndEs(compareMap,relationId)==true)
	  {
		  System.out.println("同步更新成功");
	  }
	  else {
		  System.out.println("同步更新失败");
	  }	
	}		
	
	//更新sanban_company_info表(副表)的gslx字段
	@org.junit.jupiter.api.Test
	void testBasicInfoUpdateInfoGslx() throws Exception
	{
	  String collectionName="gs_company";
	  String colleactionRelationName="sanban_company_info";
	  Map ids=new HashMap();
	  TestDifferentUtil testDifferentUtil=new TestDifferentUtil();
	  //需要更新的key和value
	  Map newValue=new HashMap();
	  ArrayList sseLabel=new ArrayList();
	  newValue.put("gslx", "其他股份有限公司(非上市)");
	  updateToMongo(colleactionRelationName, conditionRelationUpdate, newValue);
	  //拿到更新后的数据
	  mongoRecord=srcDataSource.query(collectionName, conditionUpdate);
	  targetMongoRecord=testDifferentUtil.getTargetCompanyStoreMongoRecord(mongoRecord, srcDataSource);
	  for(int i=0;i<targetMongoRecord.size();i++)
	  {
		  ids.put(i+1, targetMongoRecord.get(i).getData().get("company_id"));
	  }	
	  //更新时间过长
	  Thread.sleep(300000);
	  boolean toObjectId=true;
	  getESUpdate(ids, "basicInfo_test",toObjectId);
	  Map compareMap=new HashMap();
  	  //设置需要比对的字段
  	  compareMap.put(0,"basicInfo;basicInfo");
  	  String relationId="company_id";
	  if(compareMongoAndEs(compareMap,relationId)==true)
	  {
		  System.out.println("同步更新成功");
	  }
	  else {
		  System.out.println("同步更新失败");
	  }	
	}		
	
	//测试插入Mongo的API方法
	public void insertToMongo(String collectionName,Map conditonal) throws Exception
	{
		  List<Record> gtRecords=srcDataSource.query(collectionName, conditonal);
		  System.out.println(gtRecords.size());
		  //System.out.println("A");
	  	  //将last_modified_time设置为null，并添加上_id属性
	  	  for(int i=0;i<gtRecords.size();i++)
	  	  {		  
	  		  //System.out.println(gtRecords.get(i).getData());
	  		  gtRecords.get(i).getData().put("last_modified_time",new BSONTimestamp());
	  		  //新增gs的记录需要构造company_id和_id一致
	  		  gtRecords.get(i).getData().put("company_id",new Long("21110"+i));
	  		  //gtRecords.get(i).getData().put("suspect_id",new Long("4000"+i));
	  		  //System.out.println(gtRecords.get(i).getData().get("company_id").getClass());
	  		  String fullname=(String) gtRecords.get(i).getData().get("fullname");
	  		  //gtRecords.get(i).getData().put("fullname", fullname+"蔡");
	  		  if(gtRecords.get(i).getData().get("_id")!=null)
	  		  {
	  			 gtRecords.get(i).getData().put("_id",null);
	  			 gtRecords.get(i).getData().remove("_id");
	  		  }
	  		  gtRecords.get(i).getData().put("_id",new ObjectId(makeId(new Long("21110"+i))));
	  	  }
	  	  this.mongoRecord=gtRecords;
	  	  
	  	  OperationResult result=srcDataSource.insert(collectionName, gtRecords);
	  	  /*
	  	  this.ids=result.getResultIds();
	  	
	  	  for(int i=0;i<ids.size();i++)
	  	  {
	  		  //System.out.println(ids.get(i+1));
	  		  this.mongoRecord.get(i).getData().put("_id", ids.get(i+1));
	  	  }*/
	  	  //Thread.sleep(10000);
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
		
	//同步更新比对
	public boolean compareMongoAndEs(Map compareMap,String relationId)
	{
		TestDifferentUtil testUtil=new TestDifferentUtil();
		System.out.println(targetMongoRecord.size());
		System.out.println(esRecord.size());
		//可能出现的问题是，同一个suspect_id比较了多次
		for(int i=0;i<targetMongoRecord.size();i++)
		{
			Object id=targetMongoRecord.get(i).getData().get(relationId);
			Map esMap= (Map) esRecord.get(id);
			Map esCompareMap=new HashMap();
			Map mongoMap=new HashMap();
			for(int j=0;j<compareMap.size();j++)
			{
				String compare[]=((String) compareMap.get(j)).split(";");
				String mongoName=compare[0];
				String esSourceName=compare[1];
				//System.out.println(mongoRecord.get(i).getData().get(mongoName));
				mongoMap.put(esSourceName, targetMongoRecord.get(i).getData().get(mongoName));
				esCompareMap.put(esSourceName, esMap.get(esSourceName));
			}
			//输出操作后mongo的属性值
			//String sourceJsonStrMongo=JSON.toJSONString(mongoMap);
			//System.out.println(mongoMap);
			//System.out.println(sourceJsonStrMongo);	
			//System.out.println(esMap);
			System.out.println(esCompareMap);
			if(!testUtil.compareMap(mongoMap, esCompareMap))
			{
				return false;
			}
		}
		return true;
	}
	
	public void getESUpdate(Map ids,String destName,boolean toObjectId) throws IOException {
		CloseableHttpClient httpClient;
		String Uri=esIp+destName+"/article/";
		for(int i=0;i<ids.size();i++)
		{
			httpClient=HttpClients.createDefault();
			Uri=esIp+destName+"/article/";
			Object id=ids.get(i+1);
			if(toObjectId==true)
			{
				String idstr=makeId(id);
				System.out.println(idstr.length());
				Uri=Uri+idstr;
			}else
			{
				Uri=Uri+id;
			}	
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
			System.out.println(source);		
			//System.out.println(responsjseBody);
			if(jsonObject!=null)
			this.esRecord.put(ids.get(i+1), source);
			httpClient.close();
		}
	}
	public String makeId(Object id)
	{
		String idStr=id.toString();
		int length=idStr.length();
		for(int i=0;i<24-length;i++)
		{
			idStr="0"+idStr;
		}
		return idStr;
	}
}
