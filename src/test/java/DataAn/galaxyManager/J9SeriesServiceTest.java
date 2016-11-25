package DataAn.galaxyManager;

import java.util.List;
import java.util.Map;

import org.bson.Document;
import org.junit.Before;
import org.junit.Test;

import DataAn.common.utils.JJSON;
import DataAn.common.utils.UUIDGeneratorUtil;

public class J9SeriesServiceTest {

	private J9SeriesService j9SeriesService;
	private String filePath = "E:\\data\\flywheel\\2000\\01\\j9-02--2000-01-01.csv";
	
	@Before
	public void init(){
		j9SeriesService = new J9SeriesService();
	}
	
	@Test
	public void readCSVFileToDoc() throws Exception{
		String versions = UUIDGeneratorUtil.getUUID();
		List<Document> docList = j9SeriesService.readCSVFileToDoc(filePath, versions);
		for (Document doc : docList) {
			Map<String, Object> content= JJSON.get().parse(doc.toJson());
			System.out.println(content);
		}
	}
	@Test
	public void readCSVFileOfJsonToDoc() throws Exception{
		List<Document> docList = j9SeriesService.readCSVFileOfJsonToDoc(filePath);
		for (Document doc : docList) {
			System.out.println(doc.toJson());
		}
	}
}
