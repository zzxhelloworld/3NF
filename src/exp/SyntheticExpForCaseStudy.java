package exp;

import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;


import object.SchemaBCNF;
import util.DBUtils;
import util.Utils;
import object.Schema3NF;
import object.Key;
import object.FD;

/**
 * This is an experiments for case study.
 * Given atomic closure:
	atomic closure: 
	###########
	[B, C, D] -> [A]
	[A, B] -> [C]
	[B, C, D] -> [E]
	[E, C] -> [D]
	[E, D] -> [C]
	[B, C, E] -> [A]
	[B, D, E] -> [A]
	[A, B, D] -> [E]
	[A, B, E] -> [D]
	###########
 * We have some decomposition results for 1)iCONF_minf_maxk & 2) iCONF_maxk_minf
 * we use the update performance to show what we gain.
 *
 */
public class SyntheticExpForCaseStudy {
	/**
	 * 2 3NF & 1 BCNF
	 * @return index 0: a list of 3NF; index 1: a list of BCNF
	 */
	public static List<Object> getDecompResult_iCONF(){
		/**
		 *  BCNF schema: 
			Schema: [A, B, D, E]
			Keys: 
			[B, D, E]
			[A, B, D]
			[A, B, E]
		 */
		List<String> s1 = Arrays.asList("A", "B", "D", "E");
		Key k11 = new Key(Arrays.asList("B", "D", "E"));
		Key k12 = new Key(Arrays.asList("A", "B", "D"));
		Key k13 = new Key(Arrays.asList("A", "B", "E"));
		SchemaBCNF bcnf1 = new SchemaBCNF(s1, Arrays.asList(k11, k12, k13));
		
		/**
		 *  BCNF schema: 
			Schema: [E, C, D]
			Keys: 
			[E, D]
			[E, C]
		 */
		List<String> s2 = Arrays.asList("E", "C", "D");
		Key k21 = new Key(Arrays.asList("D", "E"));
		Key k22 = new Key(Arrays.asList("C", "E"));
		SchemaBCNF bcnf2 = new SchemaBCNF(s2, Arrays.asList(k21, k22));
		
		/**
		 *  3NF schema with mixed cover: 
			Schema: [B, C, D, A]
			FDs: 
			[B, A] -> [C]
			Keys: 
			[B, A, D]
			[B, C, D]
		 */
		List<String> s3 = Arrays.asList("A", "B", "C", "D");
		FD fd31 = new FD(Arrays.asList("A", "B"), Arrays.asList("C"));
		Key k31 = new Key(Arrays.asList("A", "B", "D"));
		Key k32 = new Key(Arrays.asList("B", "C", "D"));
		Schema3NF thirdnf1 = new Schema3NF(s3, Arrays.asList(fd31), Arrays.asList(k31, k32));
		
		List<Object> output = new ArrayList<>();
		output.add(Arrays.asList(thirdnf1));
		output.add(Arrays.asList(bcnf1, bcnf2));
		return output;
	}
	
	/**
	 * only have iCONF decomposition's 3NF sub-schemata with FD cover
	 * @return
	 */
	public static List<Object> getDecompResult_iCONF_3NF_fdcover(){
		
		/**
		 *  3NF schema with FD cover: 
			3NF schema: 
			Schema: [B, C, D, A]
			FDs: 
			[A, B] -> [C]
			[B, C, D] -> [A]
		 */
		List<String> s3 = Arrays.asList("A", "B", "C", "D");
		FD fd31 = new FD(Arrays.asList("A", "B"), Arrays.asList("C"));
		FD fd32 = new FD(Arrays.asList("B", "C", "D"), Arrays.asList("A"));
		Schema3NF thirdnf1 = new Schema3NF(s3, Arrays.asList(fd31, fd32));
		
		List<Object> output = new ArrayList<>();
		output.add(Arrays.asList(thirdnf1));
		output.add(Arrays.asList());
		return output;
	}
	
	/**
	 * 1 3NF & 2 BCNF
	 * @return index 0: a list of 3NF; index 1: a list of BCNF
	 */
	public static List<Object> getDecompResult_CONF(){
		/**
		 *  BCNF schema: 
			Schema: [B, D, E, A]
			Keys: 
			[B, E, A]
			[B, D, E]
			[A, B, D]
		 */
		List<String> s1 = Arrays.asList("A", "B", "D", "E");
		Key k11 = new Key(Arrays.asList("A", "B", "E"));
		Key k12 = new Key(Arrays.asList("B", "D", "E"));
		Key k13 = new Key(Arrays.asList("A", "B", "D"));
		SchemaBCNF bcnf1 = new SchemaBCNF(s1, Arrays.asList(k11, k12, k13));
		
		/**
		 *  BCNF schema: 
			Schema: [A, B, C]
			Keys: 
			[A, B]
		 */
		List<String> s2 = Arrays.asList("A", "B", "C");
		Key k21 = new Key(Arrays.asList("A", "B"));
		SchemaBCNF bcnf2 = new SchemaBCNF(s2, Arrays.asList(k21));
		
		/**
		 *  3NF schema with mixed cover: 
			Schema: [B, C, D, E]
			FDs: 
			[E, C] -> [D]
			[D, E] -> [C]
			Keys: 
			[D, E, B]
			[B, C, D]
			[E, C, B]
		 */
		List<String> s3 = Arrays.asList("B", "C", "D", "E");
		FD fd31 = new FD(Arrays.asList("C", "E"), Arrays.asList("D"));
		FD fd32 = new FD(Arrays.asList("D", "E"), Arrays.asList("C"));
		Key k31 = new Key(Arrays.asList("B", "D", "E"));
		Key k32 = new Key(Arrays.asList("B", "C", "D"));
		Key k33 = new Key(Arrays.asList("B", "C", "E"));
		Schema3NF thirdnf1 = new Schema3NF(s3, Arrays.asList(fd31, fd32), Arrays.asList(k31, k32, k33));
		
		List<Object> output = new ArrayList<Object>();
		output.add(Arrays.asList(thirdnf1));
		output.add(Arrays.asList(bcnf1, bcnf2));
		return output;
	}
	
	/**
	 * only have CONF decomposition's 3NF sub-schemata with FD cover
	 * @return
	 */
	public static List<Object> getDecompResult_CONF_3NF_fdcover(){
		
		/**
		 *  3NF schema with FD cover: 
			3NF schema: 
			Schema: [B, C, D, E]
			FDs: 
			[E, C] -> [D]
			[B, C, D] -> [E]
			[E, D] -> [C]
		 */
		List<String> s3 = Arrays.asList("B", "C", "D", "E");
		FD fd31 = new FD(Arrays.asList("C", "E"), Arrays.asList("D"));
		FD fd32 = new FD(Arrays.asList("D", "E"), Arrays.asList("C"));
		FD fd33 = new FD(Arrays.asList("B", "C", "D"), Arrays.asList("E"));
		Schema3NF thirdnf1 = new Schema3NF(s3, Arrays.asList(fd31, fd32, fd33));
		
		List<Object> output = new ArrayList<>();
		output.add(Arrays.asList(thirdnf1));
		output.add(Arrays.asList());
		return output;
	}
	
	public static List<List<String>> createSpecificTuplesOfMinArmRel(List<List<String>> minArmRel,int insertNum,int startIndex){
		List<List<String>> insert = new ArrayList<List<String>>();
		exit :
		while(true) {
			for(List<String> tuple : minArmRel) {
				ArrayList<String> t = new ArrayList<String>();
				for(String value : tuple) {
					t.add(value+"_"+startIndex);
				}
				insert.add(t);
				if(insert.size() >= insertNum)
					break exit;
			}
			startIndex ++;
		}
		return insert;
	}
	
	/**
	 * run a single experiment on  insertion
	 */
	public static List<Double> runSingleExp(String output, int repeat,List<String> R, List<Key> keys, List<FD> fds, String tableName,int arm_rel_num, List<Integer> insert_num_list) throws SQLException {
		System.out.println("\n###############################\n");
		List<Double> result = new ArrayList<Double>();
		List<String> uniqueIDs = new ArrayList<>();
		String triggerID = "tri_"+tableName;
		
		//create projection table on database
		DBUtils.createTable("freeman", tableName, R);
				
		List<List<String>> armRel = DBUtils.genInsertedDataset(10, R.size());
		
		List<List<String>> ArmstrongRelation = createSpecificTuplesOfMinArmRel(armRel, arm_rel_num, 0);
				
		//make a table with data
		DBUtils.insertData("freeman", tableName, ArmstrongRelation);
		
		//add unique constraints
		System.out.println("add trigger and unique constraints...");
		int uc_id = 0;
		for(Key k : keys) {
			String uc_name = "uc_"+tableName+"_"+uc_id ++;
			uniqueIDs.add(uc_name);
			DBUtils.addUnique("freeman", k,tableName,uc_name);
		}
		
		//add FD triggers if exists
		if(!fds.isEmpty()) {
			DBUtils.addTrigger("freeman", fds, tableName, triggerID);
		}
		
		
		//update experiment
		for(Integer insert_row_num : insert_num_list) {
			List<Double> cost_list = new ArrayList<Double>();
			List<List<String>> syntheData = createSpecificTuplesOfMinArmRel(armRel, insert_row_num, ArmstrongRelation.size()/armRel.size() + 1);
			for(int i = 0;i < repeat;i ++) {
				double cost = DBUtils.insertData("freeman", tableName, syntheData);
				cost_list.add(cost);
				DBUtils.deleteData("freeman", tableName,"`id` > "+ArmstrongRelation.size());
			}
			result.add(Utils.getAve(cost_list));
			result.add(Utils.getMedian(cost_list));
		}
		
		//drop the table
		DBUtils.dropTable("freeman", tableName);
		
		//output
		String stat = "";
		for(double a : result) {
			stat += ","+a;
		}
		String res = tableName+","+ArmstrongRelation.size()+","+keys.size()+","+fds.size()+stat;
		Utils.writeContent(Arrays.asList(res), output, true);
		
		System.out.println(res.replace(",", " | "));
		System.out.println("###############################\n");
		
		return result;
	}
	
	public static void runExps(List<Object> decomp, String output, String tableName, int repeat,List<Integer> arm_rel_num_list, List<Integer> insert_num_list) throws SQLException {
		List<Schema3NF> subschema_3nf = (List<Schema3NF>) decomp.get(0);
		List<SchemaBCNF> subschema_bcnf = (List<SchemaBCNF>) decomp.get(1);
		
		for(int arm_rel_num : arm_rel_num_list) {
			for(Schema3NF schema : subschema_3nf) {
				SyntheticExpForCaseStudy.runSingleExp(output, repeat, schema.getAttr_set(), schema.getMin_key_list(), schema.getFd_set(), tableName, arm_rel_num, insert_num_list);
			}
			
			for(SchemaBCNF schema : subschema_bcnf) {
				SyntheticExpForCaseStudy.runSingleExp(output, repeat, schema.getAttr_set(), schema.getMin_key_list(), new ArrayList<FD>(), tableName, arm_rel_num, insert_num_list);
			}
		}
		
	}
	
	public static void main(String[] args) throws SQLException {
		List<Object> iCONF_decomp = SyntheticExpForCaseStudy.getDecompResult_iCONF();
		List<Object> CONF_decomp = SyntheticExpForCaseStudy.getDecompResult_CONF();
//		List<Object> iCONF_decomp = SyntheticExpForCaseStudy.getDecompResult_iCONF_3NF_fdcover();
//		List<Object> CONF_decomp = SyntheticExpForCaseStudy.getDecompResult_CONF_3NF_fdcover();
		
		String outputPath = args[0];
		String tableName = args[1];
		int repeat = Integer.parseInt(args[2]);
		int num = Integer.parseInt(args[3]);//1m,10m,100m
		List<Integer> arm_rel_num_list = Arrays.asList(num);//Armstrong relation number
		List<Integer> insert_num_list = Arrays.asList(Integer.parseInt(args[4]));
		
		//experiment for iCONF decomposition
		SyntheticExpForCaseStudy.runExps(iCONF_decomp, outputPath, tableName+"_iconf", repeat, arm_rel_num_list, insert_num_list);
		
		//experiment for CONF decomposition
		SyntheticExpForCaseStudy.runExps(CONF_decomp, outputPath, tableName+"_conf", repeat, arm_rel_num_list, insert_num_list);
	}

}
