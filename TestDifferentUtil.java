package com.szu.esu.cn;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import org.bson.Document;

import com.alibaba.fastjson.JSONObject;
import com.cninfo.mongodb2esexportsystem.bean.Record;
import com.cninfo.mongodb2esexportsystem.db.MongodbDataSource;
import com.cninfo.mongodb2esexportsystem.db.table.CompanyTypeTable;
import com.cninfo.mongodb2esexportsystem.db.table.CptAnlsTable;
import com.cninfo.mongodb2esexportsystem.db.table.GsCompanyTable;
import com.cninfo.mongodb2esexportsystem.db.table.InvesttradeTable;
import com.cninfo.mongodb2esexportsystem.db.table.PersonTable;
import com.cninfo.mongodb2esexportsystem.db.table.SanbanCompanyCustomerTable;
import com.cninfo.mongodb2esexportsystem.db.table.SanbanCompanyInfoTable;
import com.cninfo.mongodb2esexportsystem.db.table.SanbanCompanySupplierTable;
import com.cninfo.mongodb2esexportsystem.db.table.TbCompanyTable;
import com.cninfo.mongodb2esexportsystem.db.table.bean.CptAnlsBean;
import com.cninfo.mongodb2esexportsystem.db.table.bean.DirectorsOfHighBean;
import com.cninfo.mongodb2esexportsystem.db.table.bean.GsCompanyBean;
import com.cninfo.mongodb2esexportsystem.db.table.bean.InvesttradeBean;
import com.cninfo.mongodb2esexportsystem.db.table.bean.PersonBean;
import com.cninfo.mongodb2esexportsystem.db.table.bean.SanbanCompanyCustomerBean;
import com.cninfo.mongodb2esexportsystem.db.table.bean.SanbanCompanyInfoBean;
import com.cninfo.mongodb2esexportsystem.db.table.bean.SanbanCompanySupplierBean;
import com.cninfo.mongodb2esexportsystem.db.table.bean.TbCompanyBean;
import com.cninfo.mongodb2esexportsystem.util.NumberUtil;
import com.cninfo.mongodb2esexportsystem.util.PinyinUtil;
import com.cninfo.mongodb2esexportsystem.util.TimeUtil;

public class TestDifferentUtil {

	//该方法需要根据最新的转换规则进行调整
	//将新增的Person的Record进行必要的转换
	public List<Record> getTargetPersonMongoRecord(List<Record> records,MongodbDataSource mongodbDataSource) throws Exception
	{
		List<Record> targetRecords=new ArrayList<Record>();
		
		for(int i=0;i<records.size();i++)
		{
			//拿到原始的record记录
			Record record=records.get(i);
			Object suspectId = record.getData().get("suspect_id");
			Record targetRecord=new Record(suspectId,record.getState(), record.getData(), record.getOwnUser(),
					record.getOwnModule(),record.getCreatTimeStamp(),record.getOtherInfo());
			targetRecord.getData().clear();
			Map targetDataMap=targetRecord.getData();
			targetDataMap.put("suspect_id", suspectId);
			PersonTable personTable = PersonTable.getInstance(mongodbDataSource);
			PersonBean personInfo = personTable.getpersonInfo((Long) suspectId);
			String name = personInfo.getName();
			String birth = personInfo.getBirthday();
			String highestEducation = personInfo.getEducation();
			String sex = personInfo.getSex();
			String nationality = personInfo.getNationality();
			ArrayList<String> workExperiences = personInfo.getWork_experience();
			ArrayList<String> educatExperience = personInfo.getEducat_experience();
			ArrayList<Long> companyIdList = personInfo.getCompanys();
			ArrayList<String> companyNameList = personInfo.getCompanyNames();
			ArrayList<JSONObject> holder = personInfo.getHolder();
			ArrayList<JSONObject> seniorExecutive = personInfo.getSenior_executive();
			ArrayList<JSONObject> schoolAndMajor = personInfo.getSchoolAndMajor();
			ArrayList<String> personWorkInfo = personInfo.getPerson_work_info();
			String resume = personInfo.getResume();	
			String rsd_resume_parse = personInfo.getRsd_resume_parse();
			if (name != null) {
				targetDataMap.put("personName", name);
				targetDataMap.put("namePinyin", PinyinUtil.toFullAndShortPinyin(name));
			}
			JSONObject head = new JSONObject();
			if (birth != null) {
				head.put("birthday", birth);
			}
			if (highestEducation != null) {
				head.put("highestAcademicDegree", highestEducation);
			}
			if (sex != null) {
				head.put("gender", sex);
			}
			if (nationality != null) {
				head.put("nationality", nationality);
			}
			targetDataMap.put("basicInfo", head.toJSONString());
			// 名下公司id
			targetDataMap.put("companyIds", companyIdList);
			// 教育背景
			if (schoolAndMajor != null) {
				targetDataMap.put("educationBackground", schoolAndMajor.toString());
			}
			// 工作经历
			if (personWorkInfo.size() > 0) {
				targetDataMap.put("workExperience", personWorkInfo);
			}
			// 任职背景
			if (workExperiences != null) {
				targetDataMap.put("companyNames", workExperiences);
			}
			// 学校背景
			if (educatExperience != null) {
				targetDataMap.put("schoolNames", educatExperience);
			}
			if (resume != null) {
				targetDataMap.put("resume", resume);
			}
			// 企业数
			targetDataMap.put("companyNo", companyIdList.size());
			ArrayList<Integer> companyTypes = new ArrayList<Integer>();
			// 名下公司所有省份
			ArrayList<String> provinces = new ArrayList<String>();
			// 名下公司所有行业
			ArrayList<String> sseIndustrys5 = new ArrayList<String>();
			for (Long companyId : companyIdList) {
				GsCompanyTable gsCompanyTable = null;
				gsCompanyTable = GsCompanyTable.getInstance(mongodbDataSource);
				GsCompanyBean gsCompanyInfoBean = gsCompanyTable
						.getGs_Company_info(String.valueOf(new Long(companyId)));
				Integer companyType = gsCompanyInfoBean.getCompany_type();
				String province = gsCompanyInfoBean.getProvince();
				String sseIndustry = gsCompanyInfoBean.getSse_industry();
				if (companyType != null) {
					companyTypes.add(companyType);
				}
				if (province != null && !province.equals("")) {
					provinces.add(province);
				}
				if (sseIndustry != null && !sseIndustry.equals("")) {
					sseIndustrys5.add(sseIndustry);

				}
			}
			// 选出最小的
			if (companyTypes.size() > 0) {
				Integer minCompanyType = null;
				if (companyTypes.contains(1)) {
					minCompanyType = new Integer(1);
				} else if (companyTypes.contains(8)) {
					minCompanyType = new Integer(3);
				} else {
					minCompanyType = new Integer(99);
				}
				targetDataMap.put("minCompanyType", minCompanyType);
			}
			// 去重，名下所有企业的所在省份
			ArrayList<String> provincesRemoveRepeat = removeRepeat(provinces);
			if (provincesRemoveRepeat.size() > 0) {
				targetDataMap.put("province", provincesRemoveRepeat);
			}
			//targetDataMap.put("province", provinces);
			// 去重，行业
			ArrayList<String> removeReIndustry_5 = removeRepeat(sseIndustrys5);
			if (removeReIndustry_5.size() > 0) {
				targetDataMap.put("industry5th", removeReIndustry_5);
			}
			//targetDataMap.put("sseIndustry", sseIndustrys5);
			// 相关企业
			JSONObject RelatedEnterprises = new JSONObject();
			SanbanCompanyInfoTable sanbanCompanyInfoTable = SanbanCompanyInfoTable.getInstance(mongodbDataSource);
			ArrayList<JSONObject> legalPersons = sanbanCompanyInfoTable.getLegalPerson(companyIdList, name);
			TbCompanyTable.getInstance(mongodbDataSource).addSalaryAndStock(seniorExecutive, name);
			TbCompanyTable.getInstance(mongodbDataSource).addSalaryAndStock(holder, name);
			TbCompanyTable.getInstance(mongodbDataSource).addSalaryAndStock(legalPersons, name);
			if (holder != null) {
				RelatedEnterprises.put("holder", holder);
			}
			if (seniorExecutive != null) {
				RelatedEnterprises.put("senior_executive", seniorExecutive);
			}
			if (legalPersons != null) {
				RelatedEnterprises.put("legal_person", legalPersons);
			}
			targetDataMap.put("relatedCompany", RelatedEnterprises.toJSONString());		
			targetDataMap.put("rsdResumeParse",rsd_resume_parse);	
			targetRecords.add(targetRecord);
			
		} 
		return targetRecords;
	}


	//该方法需要根据最新的转换规则进行调整
	//将新增的basicInfo的Record进行必要的转换
	/*
	 * 参数说明：
	 * records：修改的记录或者是新增的记录，进行必要的转换，方便于ES的数据进行比对
	 * mongodbDataSource：Mongo数据源
	 */
	public List<Record> getTargetCompanyStoreMongoRecord(List<Record> records,MongodbDataSource mongodbDataSource) throws Exception
	{
		List<Record> targetRecords=new ArrayList<Record>();
		
		for(int i=0;i<records.size();i++)
		{
			//拿到原始的record记录
			Record record=records.get(i);
			Object companyId = record.getData().get("company_id");
			Record targetRecord = record.clone();
			System.out.println(companyId);
			targetRecord.getData().clear();
			Map targetDataMap = targetRecord.getData();
			
			// 获取gs_company表中的相关数据
			GsCompanyBean gsCompanyBean = GsCompanyTable.getInstance(mongodbDataSource)
					.getGsCompanyBeanForCompanyStore(companyId.toString());
			String fullname = gsCompanyBean.getFullname();
			String sseIndustry = gsCompanyBean.getSse_industry();
			String zch = gsCompanyBean.getZch();
			String tym = gsCompanyBean.getTym();
			
			// 获取sanban_company_info表中的相关数据
			SanbanCompanyInfoBean sanban_company_info_bean = SanbanCompanyInfoTable.getInstance(mongodbDataSource)
					.getSanbanCompanyInfoBean((Long) companyId);
			String address = sanban_company_info_bean.getAddress();
			String business = sanban_company_info_bean.getBusiness();
			String company_homepage = sanban_company_info_bean.getCompany_homepage();
			String email = sanban_company_info_bean.getEmail();
			String phone = sanban_company_info_bean.getPhone();
			String clrq = sanban_company_info_bean.getClrq();
			String finance = sanban_company_info_bean.getFinance();
			String financeType = sanban_company_info_bean.getFinancetype();
			String jyzt = sanban_company_info_bean.getJyzt();
			String legalperson = sanban_company_info_bean.getLegalperson();
			String gslx = sanban_company_info_bean.getGslx();
			String hzrq = sanban_company_info_bean.getHzrq();
			String djjg = sanban_company_info_bean.getDjjg();
			String yyqx = sanban_company_info_bean.getYyqx();
			String yyqxz = sanban_company_info_bean.getYyqxz();
			String loginaddress = sanban_company_info_bean.getLoginaddress();
			
			// 现任董监高，离职的来自证券
			ArrayList<DirectorsOfHighBean> directorsOfHighs = PersonTable.getInstance(mongodbDataSource)
					.getDirectorsOfHigh(Long.valueOf(companyId.toString()));

			// 获取tb_company相关信息
			ArrayList<TbCompanyBean> tbCompanyBeans = TbCompanyTable.getInstance(mongodbDataSource)
					.getTb_company_bean(Long.valueOf(companyId.toString()));
			ArrayList<String> mainBusinesss = new ArrayList<String>();
			for (TbCompanyBean tbCompanyBean : tbCompanyBeans) {
				String mainBusiness = tbCompanyBean.getMain_business();
				mainBusinesss.add(mainBusiness);
			}
			
			// 获取法人的公司数目与suspect_id
			Long[] legalpersonCompanyNumberAndSuspectId = PersonTable.getInstance(mongodbDataSource)
					.getLegalpersonCompanyNumberAndSuspectId(Long.valueOf(companyId.toString()), legalperson);
			Long legalpersonCompanyNumber = legalpersonCompanyNumberAndSuspectId[0];
			Long legalpersonCompanySuspectId = legalpersonCompanyNumberAndSuspectId[1];

			CptAnlsBean cptAnlsBean = CptAnlsTable.getInstance(mongodbDataSource)
					.getCptAnlsBean(Long.valueOf(companyId.toString()));
			
			// 加入header
			JSONObject Header = new JSONObject();
			Header.put("fullname", fullname);
			targetDataMap.put("fullname", fullname);
			Header.put("address", address);
			// Header.put("major_business", main_business_s);
			Header.put("homepage", company_homepage);
			Header.put("email", email);
			Header.put("phone", phone);
			Header.put("sseIndustry", sseIndustry);
			Header.put("stock_id_and_stock_name", tbCompanyBeans);

			if (tym != null) {

				Header.put("unifiedCode", tym);
				try {
					Header.put("organizationCode",
							new StringBuffer((new StringBuffer(tym).reverse().substring(1, 10))).reverse());
				} catch (Exception e) {
				}
			}

			Header.put("companyType", gslx);
			Header.put("registrationAuthority", djjg);
			Header.put("business", business);
			Header.put("yyqx", yyqx);
			Header.put("yyqxz", yyqxz);
			Header.put("changePermitDate", hzrq);
			Header.put("mainBusiness", mainBusinesss);
			Header.put("registeredAddress", loginaddress);
			targetDataMap.put("basicInfo", Header.toJSONString());
			
			// 加入Registration_information
			JSONObject registrationInformation = new JSONObject();
			registrationInformation.put("registrationMark", zch);
			registrationInformation.put("registrationDate", clrq);
			if (financeType != null&&!financeType.contains("经营范围")) {
				registrationInformation.put("registeredCapital", finance+financeType);
			}else {
				registrationInformation.put("registeredCapital", finance);
			}
			registrationInformation.put("operatingStatus", jyzt);

			targetDataMap.put("registrationInfo", registrationInformation.toJSONString());
			
			// 加入legal_person
			JSONObject legalPersonEs = new JSONObject();
			legalPersonEs.put("legalPersonName", legalperson);
			legalPersonEs.put("relatedCompanyNum", legalpersonCompanyNumber);
			legalPersonEs.put("legalpersonId", legalpersonCompanySuspectId);

			targetDataMap.put("legalpersonInfo", legalPersonEs.toString());
			
			// 加入Directors_of_high
			targetDataMap.put("excutive", JSONObject.toJSONString(directorsOfHighs));

			// 加入Supplier_and_customer
			ArrayList<SanbanCompanyCustomerBean> sanbanCompanyCustomers = SanbanCompanyCustomerTable
					.getInstance(mongodbDataSource).getSanbanCompanyCustomer(Long.valueOf(companyId.toString()));
			ArrayList<SanbanCompanySupplierBean> sanbanCompanySuppliers = SanbanCompanySupplierTable
					.getInstance(mongodbDataSource).getSanbanCompanySupplier(Long.valueOf(companyId.toString()));

			JSONObject supplierAndCustomer = new JSONObject();
			supplierAndCustomer.put("customer", JSONObject.toJSON(sanbanCompanyCustomers));
			supplierAndCustomer.put("supplier", JSONObject.toJSON(sanbanCompanySuppliers));
			targetDataMap.put("supplierAndCustomer", supplierAndCustomer.toJSONString());
			// 加入竞争分析
			targetDataMap.put("competitionAnalysis", JSONObject.toJSONString(cptAnlsBean));
//			// 加入股票信息
//			ArrayList<TbCompanyBean> stockInfos = (ArrayList<TbCompanyBean>) (tbCompanyBeans.clone());
//			for (TbCompanyBean stockInfo : stockInfos) {
//				stockInfo.setListingDate(null);
//				stockInfo.setMain_business(null);
//				stockInfo.setStockMarke(null);
//			}
//			targetData.put("stock",JSONObject.toJSONString(stockInfos));
			targetRecords.add(targetRecord);
			
		}			
		return targetRecords;
	}
	//该方法需要根据最新的转换规则进行调整
	//将新增的Search的Record进行必要的转换
	public List<Record> getTargetCompanySearchMongoRecord(List<Record> records,MongodbDataSource mongodbDataSource)throws Exception	
	{
		List<Record> targetRecords=new ArrayList<Record>();
		
		for(int i=0;i<records.size();i++)
		{
			//拿到原始的record记录
			Record record=records.get(i);
			Object companyId = record.getData().get("company_id");
			Record targetRecord = record.clone();
			targetRecord.getData().clear();
			Map targetDataMap = targetRecord.getData();
			targetDataMap.put("company_id",companyId);
			// 获取gs_company表中的相关数据
			GsCompanyBean gsCompanyBean = GsCompanyTable.getInstance(mongodbDataSource)
					.getGsCompanyBeanForCompanySearch(companyId.toString());			
			String fullname = gsCompanyBean.getFullname();
			String sseIndustry = gsCompanyBean.getSse_industry();
			String sseIndustryName = gsCompanyBean.getSse_industryName();
			String zch = gsCompanyBean.getZch();
			String tym = gsCompanyBean.getTym();
			String province = gsCompanyBean.getProvince();
			ArrayList<?> othernames = gsCompanyBean.getOthernames();
			ArrayList<?> sseLable = gsCompanyBean.getSse_lable();
			Document companyattr = gsCompanyBean.getCompanyattr();
			Object companyTypeFromGsCompany = null;
			if (companyattr != null) {
				companyTypeFromGsCompany = companyattr.get("company_type");

			}
			// 获取sanban_company_info表中的相关数据
			SanbanCompanyInfoBean sanbanCompanyInfoBean = SanbanCompanyInfoTable.getInstance(mongodbDataSource)
					.getSanbanCompanyInfoBean((Long) companyId);
			String address = sanbanCompanyInfoBean.getAddress();
			String business = sanbanCompanyInfoBean.getBusiness();
			String company_homepage = sanbanCompanyInfoBean.getCompany_homepage();
			String email = sanbanCompanyInfoBean.getEmail();
			String phone = sanbanCompanyInfoBean.getPhone();
			String clrq = sanbanCompanyInfoBean.getClrq();
			String finance = sanbanCompanyInfoBean.getFinance();
			String jyzt = sanbanCompanyInfoBean.getJyzt();
			String legalperson = sanbanCompanyInfoBean.getLegalperson();
			String gslx = sanbanCompanyInfoBean.getGslx();
			String hzrq = sanbanCompanyInfoBean.getHzrq();
			String djjg = sanbanCompanyInfoBean.getDjjg();
			String yyqx = sanbanCompanyInfoBean.getYyqx();
			String yyqxz = sanbanCompanyInfoBean.getYyqxz();
			String loginaddress = sanbanCompanyInfoBean.getLoginaddress();
			// 现任董监高，离职的来自证券
			ArrayList<DirectorsOfHighBean> directorsOfHighs = PersonTable.getInstance(mongodbDataSource)
					.getDirectorsOfHigh(Long.valueOf(companyId.toString()));
			// 1主板和三板，3路演，5投资机构，7融资
			Long companyTypeFromCompanyTypeTable = CompanyTypeTable.getInstance(mongodbDataSource)
					.getCompanyType(Long.valueOf(companyId.toString()));

			ArrayList<TbCompanyBean> tb_company_beans = TbCompanyTable.getInstance(mongodbDataSource)
					.getTb_company_bean(Long.valueOf(companyId.toString()));
			
			InvesttradeBean investtradeBean = InvesttradeTable.getInstance(mongodbDataSource).getFinanceAmont(Long.valueOf(companyId.toString()));
			if (investtradeBean != null) {
				Double latestFinancingAmount = investtradeBean.getStepnumAmount();// 货币单位为 人民币，数字单位为 亿
				String latestFinancingInstitutions = investtradeBean.getPartner();
				String latestFinancingTime = investtradeBean.getSteptime();
				String rzStatus = investtradeBean.getStep();
				ArrayList<String> latestFinancingInstitutionIds = new ArrayList<>();
				if (investtradeBean.getInvestorid().size()>0){
					latestFinancingInstitutionIds.addAll(investtradeBean.getInvestorid());
				}
				targetDataMap.put("latestFinancingTime", TimeUtil.FormatTime(latestFinancingTime));
				targetDataMap.put("latestFinancingAmount", latestFinancingAmount);
				targetDataMap.put("latestFinancingInstitution", latestFinancingInstitutions);
				//targetDataMap.put("latestFinancingInstitutionIds", latestFinancingInstitutionIds);
				targetDataMap.put("financingRound", rzStatus);

			}
			if (fullname != null) {
				targetDataMap.put("fullname", fullname);
				targetDataMap.put("fullNamePinyin", PinyinUtil.toFullAndShortPinyin(fullname));
			}
			if (province != null) {
				targetDataMap.put("province", province);
			}
			if (othernames != null) {
				targetDataMap.put("alternativeName", othernames);
				ArrayList<StringBuilder> othernamePinyins = new ArrayList<>();
				for (Object othername : othernames) {
					if (othername != null) {
						othernamePinyins.addAll(PinyinUtil.toFullAndShortPinyin(othername.toString()));
					}
				}
				targetDataMap.put("alternativeNamePinyin", othernamePinyins);

			}
			
			if (sseLable != null) {
				targetDataMap.put("tag", sseLable);
			}
			if (sseIndustry != null) {
				targetDataMap.put("sseIndustry", sseIndustry);
			}
			/*
			if (sseIndustryName != null) {
				targetDataMap.put("sseIndustryName", sseIndustryName);
			}
			*/
			ArrayList<String> persons = new ArrayList<String>();
			for (DirectorsOfHighBean directorsOfHigh : directorsOfHighs) {
				String personName = directorsOfHigh.getName();
				persons.add(personName);

			}
			ArrayList<StringBuilder> personPinyins = new ArrayList<StringBuilder>();
			targetDataMap.put("executiveName", persons);
			for (String person : persons) {
				personPinyins.addAll(PinyinUtil.toFullAndShortPinyin(person));

			}
			targetDataMap.put("executiveNamePinyin", personPinyins);
			
			if (jyzt != null) {
				targetDataMap.put("operatingStatus",
						GsCompanyTable.getInstance(mongodbDataSource).convertOperatingStatus(jyzt));
			}
			if (clrq != null) {
				targetDataMap.put("registrationDate", TimeUtil.FormatTime(clrq));
			}
			if (legalperson != null) {
				targetDataMap.put("legalperson", legalperson);
			}

			if (finance != null) {
				targetDataMap.put("registeredCapital", NumberUtil.stringToFloat(finance));
			}
			if (tym != null) {
				targetDataMap.put("unifiedCode", tym);
			}
			if (zch != null) {
				targetDataMap.put("registrationMark", zch);
			}
			ArrayList<String> numbers = new ArrayList<>();
			ArrayList<String> stockShortNames = new ArrayList<>();
			ArrayList<String> stockMarkes = new ArrayList<>();
			ArrayList<String> stockTypes = new ArrayList<>();
			ArrayList<Long> listingDates = new ArrayList<>();
			for (TbCompanyBean tbCompanyBean : tb_company_beans) {
				numbers.add(tbCompanyBean.getStock_id());
				stockShortNames.add(tbCompanyBean.getStock_name());
				stockMarkes.add(tbCompanyBean.getStockMarke());
				stockTypes.add(tbCompanyBean.getStock_type());
				if (tbCompanyBean.getListingDate() != null) {
					listingDates.add(NumberUtil.stringToLong(tbCompanyBean.getListingDate()));
				}
			}
			targetDataMap.put("stockCode", numbers);
			targetDataMap.put("stockSymbol", stockShortNames);
			targetDataMap.put("stockMarke", stockMarkes);
			// targetData.put("public_stage", stockTypes);
			targetDataMap.put("listingDate", listingDates);

			ArrayList<Integer> companyTypeList = CompanyTypeTable.getInstance(mongodbDataSource)
					.getCompanyTypeList(Long.valueOf(companyId.toString()),numbers);
			String companyTypeCode = CompanyTypeTable.getInstance(mongodbDataSource)
					.getCompanyTypeCodeList(companyTypeList);

			// es中。1主板,2三板，3路演，5投资机构，7融资,99其他
			if (companyTypeFromCompanyTypeTable != null) {
				if (companyTypeFromGsCompany != null && companyTypeFromCompanyTypeTable.equals(1l)) {

					if (companyTypeFromGsCompany.equals("1")) {
						targetDataMap.put("companyType", 1);
					} else if (companyTypeFromGsCompany.equals("8")) {
						targetDataMap.put("companyType", 2);
					}
				} else {
					targetDataMap.put("companyType", companyTypeFromCompanyTypeTable);
				}
			} else {
				targetDataMap.put("companyType", 99);
			}
			targetRecords.add(targetRecord);		
		}	
		//System.out.println(targetRecords.size());
		return targetRecords;
	}
	
	
	
	
	
	public ArrayList<String> removeRepeat(ArrayList<String> inputList) {
		ArrayList<String> output = new ArrayList<String>();
		HashSet<String> inputSet = new HashSet<String>(inputList);
		output.addAll(inputSet);
		return output;
	}
		
	private int time=0;
	//如果获得的元素是map的话，比较map元素
	public boolean compareMap(Map mongoMap,Map esMap)
	{
		/*
		System.out.println("compareMap");
		time++;
		System.out.println("time ："+time);
		*/
		System.out.println(mongoMap.size());
		System.out.println(esMap.size());
		//第一步，比较两个map的size是否一致，不一致直接返回false
		if(mongoMap.size()!=esMap.size())
		{
			return false;
		}
		//遍历es的map
		for(Object key:esMap.keySet())
		{
			//如果map元素是map
			//System.out.println(key);
			if(esMap.get(key) instanceof Map && mongoMap.get(key) instanceof Map)
			{
				//System.out.println("元素是map");
				Map mongoElement=(Map) mongoMap.get(key);
				Map esElement=(Map) esMap.get(key);
				if(compareMap(mongoElement, esElement)==false)
				{
					return false;
				}
			}
			//如果map元素是map
			else if(esMap.get(key) instanceof List && mongoMap.get(key) instanceof List)
			{
				//System.out.println("元素是list");
				List mongoElement=(List) mongoMap.get(key);
				List esElement=(List) esMap.get(key);
				if(compareList(mongoElement, esElement)==false)
				{
					return false;
				}
			}
			//基本类型比较
			else
			{
				//System.out.println(key);
				//System.out.println("元素是基本类型");
				//System.out.println(esMap.get(key));
				//System.out.println(mongoMap.get(key));
				if(esMap.get(key)!=null&&mongoMap.get(key)!=null)
				{
					System.out.println("esMap's value: "+esMap.get(key));
					System.out.println("mongoMap's value: "+mongoMap.get(key));					
					if(esMap.get(key) instanceof Double &&mongoMap.get(key) instanceof Long)
					{
						Long value =(Long) mongoMap.get(key);
						double doubleValue=value.doubleValue();
						mongoMap.put(key, doubleValue);
					}
					else if(esMap.get(key) instanceof Long &&mongoMap.get(key) instanceof Double)
					{
						Long value =(Long) esMap.get(key);
						double doubleValue=value.doubleValue();
						esMap.put(key, doubleValue);
					}else if(esMap.get(key) instanceof Double &&mongoMap.get(key) instanceof Integer)
					{
						Integer value=(Integer)mongoMap.get(key);
						double doubleValue=value.doubleValue();
						mongoMap.put(key, doubleValue);
					}else if(esMap.get(key) instanceof Integer &&mongoMap.get(key) instanceof Double)
					{
						Integer value=(Integer)esMap.get(key);
						double doubleValue=value.doubleValue();
						esMap.put(key, doubleValue);
					}else if(esMap.get(key) instanceof Double && mongoMap.get(key) instanceof Float)
					{
						System.out.println("mongo: "+mongoMap.get(key));
						Float value=(Float)mongoMap.get(key);
						double doubleValue =Double.parseDouble(String.valueOf(value));
						System.out.println(doubleValue);
						mongoMap.put(key, doubleValue);
					}else if(esMap.get(key) instanceof Float && mongoMap.get(key) instanceof Double)
					{
						Float value=(Float)esMap.get(key);
						double doubleValue =Double.parseDouble(String.valueOf(value));
						esMap.put(key, doubleValue);
					}
					if(esMap.get(key).equals(mongoMap.get(key))==false)
					{
						System.out.println(mongoMap.get(key).getClass());
						System.out.println(esMap.get(key).getClass());
						System.out.println("esMap's value: "+esMap.get(key));
						System.out.println("mongoMap's value: "+mongoMap.get(key));
						return false;
					}
				}			
			}
	}
	//全部一致才返回true
		return true;
	}
	//如果获得的元素是list的话，比较list
	public boolean compareList(List mongoList,List esList)
	{	
		
		//第一步比较两个list的大小
		if(esList.size()!=mongoList.size())
		{
			return false;
		}
		//遍历eslist的所有元素
		for(Object esElement:esList)
		{
			if(esElement instanceof List)
			{
				boolean flagList=false;
				List esEleList=(List) esElement;
				//如果元素是list,则需要找到mongoList里面的元素是list的比较 只要有一个相等是则返回true
				for(Object mongoElement:mongoList)
				{
					if(mongoElement instanceof List)
					{
						List mongoEleList=(List) mongoElement;
						//大小相等再去递归调用
						if(mongoEleList.size()==esEleList.size())
						{							
							if(compareList(esEleList, mongoEleList)==true)
							{
								flagList=true;
								break;
							}
						}
						
					}
				}
				if(flagList==false)
				{
					return false;
				}
				
			}//如果list下的元素是map
			else if(esElement instanceof Map)
			{
				boolean flagMap=false;
				Map esEleMap=(Map) esElement;
				for(Object mongoElement:mongoList)
				{
					if(mongoElement instanceof Map)
					{
						Map mongoEleMap = (Map) mongoElement;
						if(mongoEleMap.size()==esEleMap.size())
						{
							if(compareMap(mongoEleMap, esEleMap)==true)
							{
								flagMap=true;
								break;
							}
						}
					}
				}
				if(flagMap==false)
				{
					return false;
				}
			}//如果list下的元素是其他基本元素
			else
			{
				//System.out.println(esElement.getClass());
				if(esElement instanceof Double)
				{
					boolean flagDouble=false;
					//防止两个都是Double的情况
					if(mongoList.contains(esElement)==true)
					{
						continue;
					}
					for(Object mongoElement:mongoList)
					{
						//System.out.println(mongoElement.getClass());
						if(mongoElement instanceof Long)
						{
							double mongoDouble=((Long) mongoElement).doubleValue();
							double esDouble=(double) esElement;
							if(mongoDouble==esDouble)
							{
								flagDouble=true;
								break;
							}

						}						
					}
					if(flagDouble==false)
					{
						return false;
					}
				}
				else if(esElement instanceof String)
				{
					if(mongoList.contains(esElement)==true)
					{
						System.out.println(esElement);
						continue;
					}
					String esElementStr=(String) esElement;
					StringBuilder esElementStringBuilder=new StringBuilder(esElementStr);
					boolean flagStringBuilder=false;
					for(Object mongoElement:mongoList)
					{
						
						if(mongoElement instanceof StringBuilder)
						{
							//StringBuilder对象比较，比较字符串内容是否相等
							if(mongoElement.toString().equals(esElementStringBuilder.toString()))
							{
								System.out.println(mongoElement);
								System.out.println(esElementStringBuilder);
								flagStringBuilder=true;
								break;
							}
						}
						
					}
					if(flagStringBuilder==false)
					{
						return false;
					}
				}
				else if(mongoList.contains(esElement)==false)
				{					
					System.out.println("esMap's value: "+esElement.getClass());
					System.out.println("esMap's value: "+esElement);
					return false;
				}
			}
		}
		return true;
	}	
	
	
	
	
					
}
