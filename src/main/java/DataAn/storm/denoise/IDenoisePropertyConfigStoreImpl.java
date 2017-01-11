package DataAn.storm.denoise;

import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import DataAn.storm.BaseConfig;
import DataAn.storm.StormUtils;
import DataAn.storm.zookeeper.ZooKeeperClient;
import DataAn.storm.zookeeper.ZooKeeperNameKeys;
import DataAn.storm.zookeeper.ZooKeeperClient.ZookeeperExecutor;

public class IDenoisePropertyConfigStoreImpl implements IDenoisePropertyConfigStore{

	private static Map<String,List<ParameterDto>> paramMap = new HashMap<String,List<ParameterDto>>();
	
	@Override
	public void initialize(Map<String, String> context) throws Exception {
		paramMap.clear();
		Map conf=new HashMap<>();
		BaseConfig baseConfig=null;
		baseConfig= StormUtils.getBaseConfig(BaseConfig.class);
		ZooKeeperNameKeys.setZooKeeperServer(conf, baseConfig.getZooKeeper());
		ZooKeeperNameKeys.setNamespace(conf, baseConfig.getNamespace());
		ZookeeperExecutor executor=new ZooKeeperClient()
				.connectString(ZooKeeperNameKeys.getZooKeeperServer(conf))
				.namespace(ZooKeeperNameKeys.getNamespace(conf))
				.build();
		String path = "/cfg/topDenioseConfig";
		byte[] bytes = executor.getPath(path);
		String topDenioseConfig = new String(bytes, Charset.forName("utf-8"));
		//获取所有的陀螺的x、y、z三个轴的角速度的sequence值,
		List<ParameterDto> paramlist = DenoiseUtils.getParamtoDenoiseList(topDenioseConfig);
		paramMap.put("paramList", paramlist);
		}

	@Override
	public List<ParameterDto> getParamList() {
		return paramMap.get("paramList");
	}
	
}
