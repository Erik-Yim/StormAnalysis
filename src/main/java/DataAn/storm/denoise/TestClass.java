package DataAn.storm.denoise;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import DataAn.storm.DefaultDeviceRecord;
import DataAn.storm.IDeviceRecord;
import DataAn.storm.denoise.IDenoiseFilterNodeProcessor.IDenoiseFilterNodeProcessorGetter;
import DataAn.storm.persist.IMongoPersistService.MongoPersistServiceGetter;
import DataAn.storm.persist.MongoPeristModel;
import DataAn.storm.status.ISendStatus.ISendStatusGetter;


public class TestClass {
	public static void main(String args[]){
		
//	DefaultDeviceRecord ird = new DefaultDeviceRecord();
//	ird.set_time(10);
//	ird.setId("112345");
//	ird.setName("flywheel");
//	ird.setSeries("j9");
//	ird.setStar("02");
//	ird.setTime("2016-11-01 13:47:24");
//	String[] param = new String[10];
//	String[] values = new String[10];
//	for(int i=0;i<param.length;i++){
//		param[i]= "sequence"+i;
//		values[i]=i+"";
//	}
//	ird.setProperties(param);
//	ird.setPropertyVals(values);
//	List<DefaultDeviceRecord> dd = new ArrayList<>();
//	dd.add(ird);
//	IDenoiseFilterNodeProcessorGetter iim =  new IDenoiseFilterNodeProcessorGetter();
//	iim.get().cleanup(dd);
	
		/*ISendStatusGetter ig = new ISendStatusGetter();
		Map<String,String> keyVal = new HashMap<>();
		keyVal.put("fileName", "011");
		keyVal.put("statusType", "1");
		keyVal.put("userType", "flywheel");
		//String sys = ig.get().sendStatus(keyVal);
		//System.out.println(sys);
		
		MongoPeristModel ml = new MongoPeristModel();
		ml.setSeries("j9");
		ml.setStar("star2");
		ml.setKey("02342");
		String content= "{\"name\":\"test\",\"datetime\":\"2010-02-28:00:00:00\"}";
		ml.setContent(content);
		ml.setCollections(new String[]{"test111"});		
		Map<String,String> context = new  HashMap<>();
		MongoPersistServiceGetter.getMongoPersistService(context).persist(ml, context);*/
		
		
		
		

		List<DefaultDeviceRecord> deviceRecords =new ArrayList<DefaultDeviceRecord>();
		for(int n=0;n<15;n++)
		{
			System.out.println("第几条记录："+n);
			DefaultDeviceRecord test1=new DefaultDeviceRecord();
			String[] properties =new String[10];
			String[] propertyVals=new String[10];
			for(int i=0;i<properties.length;i++){
				properties[i]= "sequence_0000"+i;
				//int n=1+(int)(Math.random()*2);
				propertyVals[i]=i%3+"."+i;
				if(i%2==0)
				{propertyVals[i]="-"+propertyVals[i];}
				if(i%3==0)
				{propertyVals[i]="-#"+propertyVals[i];}
				System.out.println("参数："+properties[i]+"值："+propertyVals[i]);
			}
			test1.setProperties(properties);
			test1.setPropertyVals(propertyVals);
			test1.setName("top");
			deviceRecords.add(test1);
		}
		cleanup(deviceRecords);
			
}
	
	public static void cleanup(List<? extends IDeviceRecord> deviceRecords) {
		try{
			String  devicename = deviceRecords.get(0).getName();
			if(devicename.equals("flywheel"))
			{
					String [] vals = null;
					String [] param = null;
					Set<String> invalid = new HashSet<>();
					for(IDeviceRecord idr:deviceRecords){
						vals = idr.getPropertyVals();
						param = idr.getProperties();
						for(int i=0;i<vals.length;i++){							
							if(vals[i].contains("#")){
								invalid.add(param[i]);
							}
						}
					}
				
				for(IDeviceRecord idr:deviceRecords){
					param = idr.getProperties();
					vals = idr.getPropertyVals();
					String [] newparam= new String[param.length-invalid.size()];
					String [] newvals= new String[param.length-invalid.size()];
					int j = 0;						
					for(int i=0;i<param.length;i++){							
						if(!(invalid.contains(param[i]))){
							newparam[j] = param[i];
							newvals[j] = vals[i];
							j++;
						}
						
					}
					((DefaultDeviceRecord)idr).setProperties(newparam);
					((DefaultDeviceRecord)idr).setPropertyVals(newvals);
				}												
			}
			
			if(devicename.equals("top"))
			{
				String [] vals = null;
				String [] param = null;
				Set<String> invalid = new HashSet<>();
				//获取所有的陀螺的x、y、z三个轴的角速度的sequence值,
				List<ParameterDto> paramlist = DenoiseUtils.getParamtoDenoiseList();
				for(IDeviceRecord idr:deviceRecords){
					vals = idr.getPropertyVals();
					param = idr.getProperties();
					for(int i=0;i<vals.length;i++){							
						if(vals[i].contains("#")){
							invalid.add(param[i]);
						}
						else
						{
							for(ParameterDto paramdto:paramlist)
							{
								System.out.println("从JSON中获取到的参数值："+paramdto.getCode()+"当前参数值："+param[i]);
								
								if(param[i].equals(paramdto.getCode()))
								{
									System.out.println("开始检测噪点,字符串转换为Double："+Double.parseDouble(vals[i]));
									if((Double.parseDouble(vals[i])<-2.2)|(2.2<Double.parseDouble(vals[i])))
									{
										invalid.add(param[i]);
										System.out.println("检测到噪点："+vals[i]);
									}
								}
							}
						}
						
					}
				}
			
			for(IDeviceRecord idr:deviceRecords){	
				param = idr.getProperties();
				vals = idr.getPropertyVals();
				String [] newparam= new String[param.length-invalid.size()];
				String [] newvals= new String[param.length-invalid.size()];
				int j = 0;						
				for(int i=0;i<param.length;i++){							
					if(!(invalid.contains(param[i]))){
						newparam[j] = param[i];
						newvals[j] = vals[i];
						j++;
					}
					
				}
				((DefaultDeviceRecord)idr).setProperties(newparam);
				((DefaultDeviceRecord)idr).setPropertyVals(newvals);
			}						
			}
			for(int i=0;i<deviceRecords.size();i++)
			{
				System.out.println("去噪之后："+i);
				for(int j=0;j<deviceRecords.get(i).getProperties().length;j++ )
				{	
					System.out.println("参数："+deviceRecords.get(i).getProperties()[j]+"值："+deviceRecords.get(i).getPropertyVals()[j]);
				}
			}
			
		}catch (Exception e) {
			throw new RuntimeException(e);
		}
	}


}
