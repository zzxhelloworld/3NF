package exp;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import nf.CONF;
import nf.DecompAlg2;
import nf.DecompAlg3;
import nf.DecompAlg4;
import nf.iCONF;
import nf.iCONFOpt_minf_maxk;
import object.DataTypeEnum;
import object.Dataset;
import object.FD;
import object.Key;
import object.Parameter;
import object.Schema3NF;
import object.SchemaBCNF;
import util.DBUtils;
import util.Utils;

/**
 * we study subschema performance on updates, which resulted from decomposition algorithms
 * the subschema we experiment will be with some FDs
 *
 */
public class SubschemaPerfExp {
	
	/**
	 * execute query and insert experiments on projection of a table on "subschema"
	 * get average/median results on "round" times experiments
	 */
	public static List<Double> exe_single_schema_exp(String NF ,int metric, String optimTarget, String decompAlg, String output, int round,List<String> subschema, List<Key> keys, List<FD> FDs, String OriginTable,String ProjTable, List<Integer> insert_row_num_list) throws SQLException, IOException {
		System.out.println("\n###############################\n");
		List<Double> result = new ArrayList<Double>();
		
		//create projection table on database
		DBUtils.createTable("freeman", ProjTable,subschema);
		
		//get projection table
		List<List<String>> proj_table_dataset = DBUtils.getProjectionForSubschema("freeman", subschema,OriginTable);
		
		//insert projection rows
		DBUtils.insertData("freeman", ProjTable,proj_table_dataset);
		
		//add unique constraints if available
		List<String> all_uc_id = new ArrayList<String>();//record all unique constraint names
		int uc_id = 0;
		for(Key k : keys) {
			String uc_name = "uc_"+ProjTable+"_"+uc_id ++;
			DBUtils.addUnique("freeman", k,ProjTable,uc_name);
			all_uc_id.add(uc_name);
		}
		
		//add FD constraints as triggers
		if(!FDs.isEmpty())
			DBUtils.addTrigger("freeman", FDs, ProjTable, "trigger_"+ProjTable);
		
		//insertion experimenst
		for(int row_num : insert_row_num_list) {
			List<Double> cost_list = new ArrayList<Double>();
			List<List<String>> inserted_data = DBUtils.genInsertedDataset(row_num,proj_table_dataset.get(0).size());
			for(int i = 0;i < round;i ++) {
				double cost = DBUtils.insertData("freeman", ProjTable,inserted_data);
				cost_list.add(cost);
				DBUtils.deleteData("freeman", ProjTable,"`id` > "+proj_table_dataset.size());
			}
			result.add(Utils.getAve(cost_list));//average time
			result.add(Utils.getMedian(cost_list));//median time
		}
		
		//drop the table
		DBUtils.dropTable("freeman", ProjTable);
		
		//output
		String stat = "";
		for(double a : result) {
			stat += ","+a;
		}
		String res_str1 = decompAlg+","+NF+","+metric+","+proj_table_dataset.size()+stat;
		Utils.writeContent(Arrays.asList(res_str1), output+"update_result_"+optimTarget+"_"+OriginTable+"_"+decompAlg+".txt", true);
		
		
		System.out.println("\nfinish the experiment! stat : "+result.toString()+"\n");
		System.out.println("###############################\n");
		
		return result;
	}
	
	public static void exe_exp(String optimTarget, int sampleLimit,String output,int round,String decompAlg,Parameter para, List<Integer> insert_row_num_list) throws Exception {
		List<String> R = Utils.getR(para);
		List<FD> atomic = Utils.computeAtomicCover(para.fd_add);//atomic closure
		List<Object> decomp = null;
		List<SchemaBCNF> schemaBCNFList = null;
		List<Schema3NF> schema3NFList = null;
		String OriginalTable = DBUtils.getDBTableName(para);
		String ProjTable = OriginalTable + "_proj";
		switch(decompAlg) {
		case "iCONFOpt-reduced minimal keyfd":
			iCONFOpt_minf_maxk optrmmix = new iCONFOpt_minf_maxk();
			decomp = optrmmix.decomp_and_output(optimTarget, para, "reduced minimal keyfd", R, atomic);
			break;
		case "iCONFOpt-optimal keyfd":
			iCONFOpt_minf_maxk optoptmix = new iCONFOpt_minf_maxk();
			decomp = optoptmix.decomp_and_output(optimTarget, para, "optimal keyfd", R, atomic);
			break;
		case "iCONF-reduced minimal":
			iCONF iconfrm = new iCONF();
			decomp = iconfrm.decomp_and_output(optimTarget, para, "reduced minimal", R, atomic);
			break;
		case "iCONF-reduced minimal keyfd":
			iCONF iconfrmmix = new iCONF();
			decomp = iconfrmmix.decomp_and_output(optimTarget, para, "reduced minimal keyfd", R, atomic);
			break;
		case "iCONF-optimal":
			iCONF iconfopt = new iCONF();
			decomp = iconfopt.decomp_and_output(optimTarget, para, "optimal", R, atomic);
			break;
		case "iCONF-optimal keyfd":
			iCONF iconfoptmix = new iCONF();
			decomp = iconfoptmix.decomp_and_output(optimTarget, para, "optimal keyfd", R, atomic);
			break;
		case "CONF":
			CONF conf = new CONF();
			decomp = conf.decomp_and_output(optimTarget, para, "original", R, atomic);
			break;
		case "CONF-mix":
			CONF confMix = new CONF();
			decomp = confMix.decomp_and_output(optimTarget, para, "original keyfd", R, atomic);
			break;
		case "DecompAlg2":
			DecompAlg2 alg2 = new DecompAlg2();
			decomp = alg2.decomp_and_output(optimTarget, para, "original", R, atomic);
			break;
		case "DecompAlg2-mix":
			DecompAlg2 alg2Mix = new DecompAlg2();
			decomp = alg2Mix.decomp_and_output(optimTarget, para, "original keyfd", R, atomic);
			break;
		case "DecompAlg3":
			DecompAlg3 alg3 = new DecompAlg3();
			decomp = alg3.decomp_and_output(optimTarget, para, "original", R, atomic);
			break;
		case "DecompAlg3-mix":
			DecompAlg3 alg3Mix = new DecompAlg3();
			decomp = alg3Mix.decomp_and_output(optimTarget, para, "original keyfd", R, atomic);
			break;
		case "DecompAlg4":
			DecompAlg4 alg4 = new DecompAlg4();
			decomp = alg4.decomp_and_output(optimTarget, para, "original", R, atomic);
			break;
		case "DecompAlg4-mix":
			DecompAlg4 alg4Mix = new DecompAlg4();
			decomp = alg4Mix.decomp_and_output(optimTarget, para, "original keyfd", R, atomic);
			break;
		}
		schemaBCNFList = (List<SchemaBCNF>) decomp.get(0);
		schema3NFList = (List<Schema3NF>) decomp.get(1);
		int count = 0;
		Map<Integer,Integer> sample_num_record_BCNF = new HashMap<Integer,Integer>();//key = key number,value = count of subschema with specific key number that has done experiments
		Map<Integer,Integer> sample_num_record_3NF = new HashMap<Integer,Integer>();//key = optimization target, value = count of subschema with specific key number that has done experiments
		for(SchemaBCNF schema : schemaBCNFList) {
			List<String> subR = schema.getAttr_set();
			List<Key> keys = schema.getMin_key_list();
			if(keys.size() == 1 && keys.get(0).size() == 0) {
				continue;
			}
			if(sample_num_record_BCNF.containsKey(keys.size())) {//check if the count exceed sample_limit for the key number
				int record = sample_num_record_BCNF.get(keys.size());
				if(record < sampleLimit) {//record
					sample_num_record_BCNF.put(keys.size(), ++ record);
				}else {
					continue;
				}
			}else {
				sample_num_record_BCNF.put(keys.size(), 1);
			}
			System.out.println("\n"+decompAlg+" | "+ count+" | BCNF | subschema : "+subR+" | key num : "+keys.size()+"\n");
			exe_single_schema_exp("BCNF", keys.size(),optimTarget, decompAlg, output, round, subR, keys, new ArrayList<>(), OriginalTable, ProjTable+"_"+decompAlg+"_"+count, insert_row_num_list);
			count ++;
		}
		for(Schema3NF schema : schema3NFList) {
			List<String> subR = schema.getAttr_set();
			List<Key> keys = schema.getMin_key_list();
			List<FD> FDs = schema.getFd_set();
			int optim_target_num;
			if(optimTarget.equals("ASN"))//attribute symbol number
				optim_target_num = Utils.compFDAttrSymbNum(FDs);
			else if(optimTarget.equals("FDN"))//FD number
				optim_target_num = FDs.size();
			else 
				optim_target_num = 0;
			if(sample_num_record_3NF.containsKey(optim_target_num)) {//check if the count exceed sample_limit for the specific number
				int record = sample_num_record_3NF.get(optim_target_num);
				if(record < sampleLimit) {//record
					sample_num_record_3NF.put(optim_target_num, ++ record);
				}else {
					continue;
				}
			}else {
				sample_num_record_3NF.put(optim_target_num, 1);
			}
			System.out.println("\n"+decompAlg+" | "+ count + " | 3NF | subschema : "+subR+" | "+optimTarget+" : "+optim_target_num+"\n");
			exe_single_schema_exp("3NF", optim_target_num, optimTarget, decompAlg, output, round, subR, keys, FDs, OriginalTable, ProjTable+"_"+decompAlg+"_"+count, insert_row_num_list);
			count ++;
		}
	}
	
	public static void main(String[] args) throws Exception {
		int round = Integer.parseInt(args[0]);//experiment repeat times
		int sampleLimit = Integer.parseInt(args[1]);//for each key/optimization target number,we sample subschemas for experiments with limited numbers
		String output = args[2];
		List<Integer> insert_row_num_list = Arrays.asList(100,200,300);
//		List<String> optimTargetList = Arrays.asList("FDN", "ASN");
//		List<String> decompAlgList = Arrays.asList("iCONF-reduced minimal","iCONF-reduced minimal keyfd",
//				"CONF", "CONF-mix", "DecompAlg2", "DecompAlg2-mix", "DecompAlg3", "DecompAlg3-mix", "DecompAlg4", "DecompAlg4-mix");
		List<String> optimTargetList = Arrays.asList("FDN");
		List<String> decompAlgList = Arrays.asList(args[3].split(","));
		//ncvoter,abalone,bridges,hepatitis
		for(String optimTarget : optimTargetList) {
			for(String decompAlg : decompAlgList) {
				Parameter para = new Parameter(new Dataset(args[4], 30, 512000, ",", "\"\"", DataTypeEnum.NULL_UNCERTAINTY), args[5]);
				SubschemaPerfExp.exe_exp(optimTarget, sampleLimit, output, round, decompAlg, para, insert_row_num_list);
			}
		}
	}

}
