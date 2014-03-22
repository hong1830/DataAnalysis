package com.dataanalysis.analysis;

public class Analysis {

	public static void main(String[] arg) throws Exception {
		
		String[] args = {"datatest","data_step1_test","data_step2_test","data_step3_add"};
//		
//		Step1_delete.run(args[0],args[1]);
//		Step2_Merger.run(args[1], args[2]);
		Step3_Add.run(args[0], args[2], args[3]);
		System.exit(0);
	}
}
