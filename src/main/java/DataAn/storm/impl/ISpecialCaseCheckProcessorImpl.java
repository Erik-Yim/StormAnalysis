package DataAn.storm.impl;

import java.util.ArrayList;
import java.util.List;

import DataAn.dto.CaseSpecialDto;
import DataAn.storm.IDeviceRecord;
import DataAn.storm.interfece.ISpecialCaseCheckProcessor;

public class ISpecialCaseCheckProcessorImpl implements
		ISpecialCaseCheckProcessor {
	public static List<CaseSpecialDto>  csDtoCatch = new ArrayList<CaseSpecialDto>();
	
	@Override
	public Object process(IDeviceRecord deviceRecord) {
		return null;

	}

	@Override
	public void persist() throws Exception {
		// TODO Auto-generated method stub

	}

}
