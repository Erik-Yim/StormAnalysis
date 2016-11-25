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
	
	@Before
	public void init(){
		j9SeriesService = new J9SeriesService();
	}
	
	@Test
	public void testInsertOne() throws Exception{
		List<MongoPeristModel> mongoPeristModels= this.getMongoPeristModelList();
		System.out.println("mongoPeristModels size: " + mongoPeristModels.size());
		IMongoPersistService mongoPersistService=  IMongoPersistService.MongoPersistServiceGetter.getMongoPersistService(null);
		try{
			long begin = System.currentTimeMillis();
			System.out.println("begin insert one...");
			for(MongoPeristModel model:mongoPeristModels){
				mongoPersistService.persist(model, null);
			}
			long end = System.currentTimeMillis();
			System.out.println("end insert one time: " + (end - begin) + " mm");
		}catch (Exception e) {
			throw e;
		}
	}
	
	@Test
	public void testInsertMany() throws Exception{
		List<MongoPeristModel> mongoPeristModels= this.getMongoPeristModelList();
		System.out.println("mongoPeristModels size: " + mongoPeristModels.size());
		IMongoPersistService mongoPersistService=  IMongoPersistService.MongoPersistServiceGetter.getMongoPersistService(null);
		try{
			long begin = System.currentTimeMillis();
			System.out.println("begin insert many...");
			mongoPersistService.persist(mongoPeristModels, null);
			long end = System.currentTimeMillis();
			System.out.println("end insert many time: " + (end - begin) +  " mm");
		}catch (Exception e) {
			throw e;
		}
	}
	private List<MongoPeristModel> getMongoPeristModelList() throws Exception{
		String filePath = "E:\\data\\flywheel\\2000\\05\\j9-02--2000-05-01.csv";
		String series = SeriesType.J9_SERIES.getName();
		String star = J9SeriesType.STRA2.getValue();
		String[] collections = {"flywheel1s", "flywheel5s", "flywheel30s", "flywheel1m"};
		String versions = UUIDGeneratorUtil.getUUID();
		System.out.println("versions: " + versions);
		List<Document> docList = j9SeriesService.readCSVFileToDoc(filePath, versions);
		System.out.println("数据总数: " + docList.size());
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
		return mongoPeristModels;
	}
}
