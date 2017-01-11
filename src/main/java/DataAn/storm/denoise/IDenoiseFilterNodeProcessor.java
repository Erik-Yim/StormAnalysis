package DataAn.storm.denoise;

import java.io.Serializable;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import DataAn.storm.BaseConfig;
import DataAn.storm.DefaultDeviceRecord;
import DataAn.storm.IDeviceRecord;
import DataAn.storm.StormUtils;
import DataAn.storm.zookeeper.ZooKeeperClient;
import DataAn.storm.zookeeper.ZooKeeperNameKeys;
import DataAn.storm.zookeeper.ZooKeeperClient.ZookeeperExecutor;
 
/**
 * 去除噪点
 * @author JIAZJ
 *
 */
public interface IDenoiseFilterNodeProcessor extends Serializable {

	void cleanup(List<? extends IDeviceRecord> deviceRecords);
	
	@SuppressWarnings("serial")
	IDenoiseFilterNodeProcessor INSTANCE=new IDenoiseFilterNodeProcessor(){

		@Override
		public void cleanup(List<? extends IDeviceRecord> deviceRecords) {
			try{
				String  devicename = "";
				String [] vals = null;
				String [] param = null;
				Set<String> invalid = new HashSet<>();
				for(IDeviceRecord idr:deviceRecords){
					if(!idr.isContent()) continue;
					devicename =idr.getName();
					//System.out.println(devicename + " beigin 去噪.. size: " + deviceRecords.size());
					//如果是飞轮
					if(devicename.equals("flywheel"))
					{
						vals = idr.getPropertyVals();
						param = idr.getProperties();
						for(int i=0;i<vals.length;i++){							
							if(vals[i].contains("#")){
								invalid.add(param[i]);
//								System.out.println("飞轮包含#参数名:"+param[i]+"---------"+vals[i]);
							}
						}
					}else if(devicename.equals("top")) //如果是陀螺
					{
						List<ParameterDto> paramlist = new IDenoisePropertyConfigStoreImpl().getParamList();
						try{
							if(paramlist == null || paramlist.size()==0){
								paramlist= DenoiseUtils.getParamtoDenoiseList();
								System.out.println("从zookeeper获取陀螺去噪参数列表失败，将从本地获取");
							}
						}
						catch(Exception e){
								e.printStackTrace();
						}
						
						vals = idr.getPropertyVals();
						param = idr.getProperties();
						for(int i=0;i<vals.length;i++){							
							if(vals[i].contains("#")){
								invalid.add(param[i]);
//								System.out.println("包含#："+param[i]+vals[i]);
							}
							else
							{
								for(ParameterDto paramdto:paramlist)
								{
									if(param[i].equals(paramdto.getCode()))
									{
										if((Double.parseDouble(vals[i])<-2.2)|(2.2<Double.parseDouble(vals[i])))
										{
//											System.out.println("大小限制："+param[i]+":"+vals[i]);
											invalid.add(param[i]);
										}
									}
								}
							}
							
						}
					}
					
				}
					
				for(IDeviceRecord idr:deviceRecords){
					if(!idr.isContent()) continue;
					param = idr.getProperties();
					vals = idr.getPropertyVals();
					String [] newparam= new String[param.length-invalid.size()];
					String [] newvals= new String[param.length-invalid.size()];
					int j = 0;						
					for(int i=0;i<param.length;i++){
						//判断当前参数不在无效参数集中
						if(!(invalid.contains(param[i]))){
							//判断是不是一个数字 字符串
							if(vals[i] != null && vals[i].matches("^[-+]?(([0-9]+)((([.]{0})([0-9]*))|(([.]{1})([0-9]+))))$")){
								newparam[j] = param[i];
								newvals[j] = vals[i];
								j++;								
							}
						}
						
					}
					((DefaultDeviceRecord)idr).setProperties(newparam);
					((DefaultDeviceRecord)idr).setPropertyVals(newvals);
				}
			}catch (Exception e) {
				throw new RuntimeException(e);
			}
		}
	};

	class IDenoiseFilterNodeProcessorGetter{
		
		public static IDenoiseFilterNodeProcessor get(){
			return INSTANCE;
		}
		
	}
	
	
}
