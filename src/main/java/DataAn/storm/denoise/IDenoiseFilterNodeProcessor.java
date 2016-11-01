package DataAn.storm.denoise;

import java.io.Serializable;
import java.util.ArrayList;
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
	
	
	class IDenoiseFilterNodeProcessorGetter{
		
		public static IDenoiseFilterNodeProcessor get(){
			return new IDenoiseFilterNodeProcessor(){

				private static final long serialVersionUID = 1L;

				@Override
				public void cleanup(List<? extends IDeviceRecord> deviceRecords) {
					String [] vals = null;
					String [] param = null;
					Set<String> invalid = new HashSet<>();
					for(IDeviceRecord idr:deviceRecords){
						if(!idr.isContent()) continue;
						vals = idr.getPropertyVals();
						param = idr.getProperties();
						for(int i=0;i<vals.length;i++){							
							if(vals[i].contains("#")){
								invalid.add(param[i]);
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
							}
							j++;
						}
						((DefaultDeviceRecord)idr).setProperties(newparam);
						((DefaultDeviceRecord)idr).setPropertyVals(newvals);
					}
					System.out.println("------------");
				}
				
			};
		}
		

		
	}
	
}
