package DataAn.storm.persist;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.bson.Document;
import org.junit.Before;
import org.junit.Test;

import DataAn.common.utils.JJSON;
import DataAn.common.utils.UUIDGeneratorUtil;
import DataAn.fileSystem.option.J9SeriesType;
import DataAn.fileSystem.option.SeriesType;
import DataAn.galaxyManager.J9SeriesService;
import DataAn.storm.kafka.Notify;

public class MongoPersistServiceTest {

	private J9SeriesService j9SeriesService;
	private String filePath = "E:\\data\\flywheel\\2000\\01\\j9-02--2000-01-01.csv";
	
	@Before
	public void init(){
		j9SeriesService = new J9SeriesService();
	}
	
	@Test
	public void test() throws Exception{
		String series = SeriesType.J9_SERIES.getName();
		String star = J9SeriesType.STRA2.getValue();
		String[] collections = {"flywheel1s", "flywheel5s", "flywheel30s", "flywheel1m"};
		String versions = UUIDGeneratorUtil.getUUID();
		System.out.println("versions: " + versions);
		List<Document> docList = j9SeriesService.readCSVFileToDoc(filePath, versions);
		System.out.println("docList size: " + docList.size());
		List<MongoPeristModel> mongoPeristModels= new ArrayList<MongoPeristModel>();
		MongoPeristModel mongoPeristModel = null;
		for (Document doc : docList) {
			mongoPeristModel = new MongoPeristModel();
			mongoPeristModel.setSeries(series);
			mongoPeristModel.setStar(star);
			mongoPeristModel.setCollections(collections);
			mongoPeristModel.setContent(doc.toJson());
			mongoPeristModels.add(mongoPeristModel);
		}
		System.out.println("mongoPeristModels size: " + mongoPeristModels.size());
		IMongoPersistService mongoPersistService=  IMongoPersistService.MongoPersistServiceGetter.getMongoPersistService(null);
		try{
			for(MongoPeristModel model:mongoPeristModels){
				mongoPersistService.persist(model, null);
			}
		}catch (Exception e) {
			throw e;
		}
	}
	
}
