package stormAnalysis;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;



public class TestSU {
	
	public static void main(String args[]){
		Object[] joblistCatch = new Object[10];
		
		for(int i=0;i<5;i++){
			List<String>  csDtoCatch = new ArrayList<>();
			joblistCatch[i] = csDtoCatch;
		}
//		Map<String,String> aa =new HashMap<>();
//		aa.put("x", "a");
//		aa.put("x", "b");
		
		for(int i=0;i<5;i++){
			List<String>  cc = (List<String>) joblistCatch[i];
			cc.add(i+"aa");
		}
		
		for(int i=0;i<5;i++){
			List<String>  cc = (List<String>) joblistCatch[i];
			
			System.out.println(cc.get(0));
		}
		
		
		
	}
}
