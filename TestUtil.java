package com.szu.esu.cn;

import java.util.List;
import java.util.Map;

public  class TestUtil {

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
				if(esMap.get(key)!=null&&mongoMap.get(key)!=null)
				{
					//System.out.println(esMap.get(key));		
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
					}else if(esMap.get(key).equals("null")&&mongoMap.get(key).equals(""))
					{
						continue;
					}
					if(esMap.get(key).equals(mongoMap.get(key))==false)
					{
						System.out.println(mongoMap.get(key).getClass());
						System.out.println(esMap.get(key).getClass());
						System.out.println("esMap's value: "+esMap.get(key));
						System.out.println("mongoMap's value: "+mongoMap.get(key));
						return false;
					}
				}else if((esMap.get(key)!=null&&mongoMap.get(key)==null)||(esMap.get(key)==null&&mongoMap.get(key)!=null))
				{
					//System.out.println(esMap.get(key).getClass());
					//System.out.println(esMap.get(key));
					//排除mongo存的为“”，在ES存为null的情况
					if(esMap.get(key)==null&&mongoMap.get(key).equals(""))
					{
						continue;
					}
					return false;
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
	
	
	
	
	
	
	
	/*
	@org.junit.jupiter.api.Test
	public void test1()
	{
		String sourceJsonStrMongo="{\"companyId\":[177534,188534,\"A\",12345],\"createTime\":\"2014.10.29\",\"patentName\":\"LED电源外壳（R50）\",\"patentOwnOrg\":\"广东科谷电源有限公司\",\"a\":{\"aa\":\"2\",\"bb\":null},\"b\":{\"aa\":\"2\",\"b\":null},\"c\":[1,2,3,[5,3,4,{\"a\":1,\"b\":2,\"c\":3}]],\"d\":[2,1,[3,{\"b\":2,\"a\":1,\"c\":5},4,5],3],\"e\":1}";
		System.out.println(sourceJsonStrMongo);
		Map jsonObject=new Gson().fromJson(sourceJsonStrMongo, Map.class);
		System.out.println(jsonObject.get("companyId") instanceof List);
		System.out.println(jsonObject.get("a") instanceof Map);
		List a=(List) jsonObject.get("c");
		List b=(List) jsonObject.get("d");
		Object e=jsonObject.get("e");
		System.out.println(e.getClass());
		//LinkedTreeMap b=(LinkedTreeMap) jsonObject.get("a");
		List list=new  ArrayList<>();
		list.add(1);
		list.add(b);
		List list2=new  ArrayList<>();
		list2.add(a);
		list2.add(1);
		for(Object object:list)
		{
			System.out.println(object.getClass());
			if(!list2.contains(object))
			{
				System.out.println("不存在该Object" +object);
			}
		}
		if(compareList(list, list2)==true)
		{
			System.out.println("存在");
		}
		if(compareList(list, list2)==false)
		{
			System.out.println("不存在");
		}
		List list3=new ArrayList();
		list3.add(1L);
		list3.add(2L);
		list3.add(3L);
		List list4=new ArrayList();
		list4.add(1.0);
		list4.add(2.0);
		list4.add(3.0);
		//list3.remove(3);
		for(Object object:list3)
		{
			System.out.println(object);
		}
		if(compareList(list3, list4))
		{
			System.out.println("一样");
		}
		//System.out.println(a.get("bb")==null);
	}
	*/
}
