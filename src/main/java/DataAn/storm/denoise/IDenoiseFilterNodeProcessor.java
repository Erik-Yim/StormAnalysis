package DataAn.storm.denoise;

import java.io.Serializable;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import DataAn.storm.DefaultDeviceRecord;
import DataAn.storm.IDeviceRecord;
 
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
					//如果是飞轮
					if(devicename.equals("flywheel"))
					{
						vals = idr.getPropertyVals();
						param = idr.getProperties();
						for(int i=0;i<vals.length;i++){							
							if(vals[i].contains("#")){
								invalid.add(param[i]);
							}
						}
					}else if(devicename.equals("top")) //如果是陀螺
					{
						//获取所有的陀螺的x、y、z三个轴的角速度的sequence值,
						List<ParameterDto> paramlist = DenoiseUtils.getParamtoDenoiseList();
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
									if(param[i].equals(paramdto.getCode()))
									{
										if((Double.parseDouble(vals[i])<-2.2)|(2.2<Double.parseDouble(vals[i])))
										{
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
						if(!(invalid.contains(param[i]))){
							newparam[j] = param[i];
							newvals[j] = vals[i];
							j++;
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
