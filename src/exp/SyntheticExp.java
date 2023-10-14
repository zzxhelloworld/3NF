package exp;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

import object.FD;
import object.Key;
import object.Schema3NF;
import util.DBUtils;
import util.Utils;

/**
 * In this experiment, with given schema and FDs, we synthesize Armstrong Relation and its copies in different sizes.
 * We then study the impact of different number of FDs and Keys on updates
 */
public class SyntheticExp {
	/**
	 * 
	 * @return a schema of varying parameter p
	 */
	public static Schema3NF getPSchema(int p) {
		if(p < 1)
			return null;
		List<String> R = new ArrayList<>();
		R.add("E");R.add("S");
		for(int i = 1;i <= p;i ++) {
			R.add("B"+i);
		}
		for(int i = 1;i <= p;i ++) {
			R.add("I"+i);
		}
		
		List<FD> FDs = new ArrayList<>();
		
		List<String> left1 = new ArrayList<>();
		List<String> right1 = Arrays.asList("E");
		for(int i = 1;i <= p;i ++) {
			left1.add("B"+i);
		}
		FDs.add(new FD(left1, right1));//FD B1,...,Bp -> E
		FDs.add(new FD(right1, left1));//FD E -> B1,...,Bp
		
		List<String> left2 = Arrays.asList("E", "S");
		List<String> right2 = new ArrayList<>();
		for(int i = 1;i <= p;i ++) {
			right2.add("I"+i);
		}
		FDs.add(new FD(left2, right2));//ES -> I1,...,Ip
		
		for(int i = 1;i <= p;i ++) {//I1 -> B1...Bp, ... , Ip -> B1...Bp
			List<String> left = Arrays.asList("I"+i);
			List<String> right = new ArrayList<>();
			for(int j = 1;j <= p;j ++) {
				right.add("B"+j);
			}
			FDs.add(new FD(left, right));
		}
		
		//generate keys
		List<Key> minKeys = new ArrayList<>();
		List<String> k1 = new ArrayList<>();//B1...BpS
		for(int i = 1; i <= p; i ++) {
			k1.add("B"+i);
		}
		k1.add("S");
		minKeys.add(new Key(k1));
		
		minKeys.add(new Key(Arrays.asList("E", "S")));//ES
		
		for(int i = 1; i <= p; i ++) {
			minKeys.add(new Key(Arrays.asList("I"+i, "S")));//I1S,...,IpS
		}
		
		
		return new Schema3NF(R, FDs, minKeys);
	}
	
	/**
	 * 
	 * @return traffic dataset's schema
	 */
	public static Schema3NF getTrafficSchema() {
		String cs = "CAR-SERIAL#";
		String lic = "LICENSE#";
		String owner = "OWNER";
		String date = "DATE";
		String time = "TIME";
		String tic = "TICKET#";
		String offe = "OFFENSE";
		//schema
		List<String> R = Arrays.asList(cs,lic,owner,date,time,tic,offe);
		//example 5.13 F from Maier book
		FD fd1 = new FD(Arrays.asList(cs),Arrays.asList(lic, owner));
		FD fd2 = new FD(Arrays.asList(lic),Arrays.asList(cs));
		FD fd3 = new FD(Arrays.asList(tic),Arrays.asList(lic, date, time, offe));
		FD fd4 = new FD(Arrays.asList(lic, date, time),Arrays.asList(tic, offe));
		List<FD> FDs = Arrays.asList(fd1,fd2,fd3,fd4);//original FD cover
		
		//keys
		List<Key> minKeys = Utils.getMinimalKeys(R, FDs);
		
		return new Schema3NF(R,FDs,minKeys);
	}
	
	/**
	 * due to minimal Armstrong relation, create some copies
	 * @param minArmRel 
	 * @param startCopyNum 
	 * @param endCopyNum 
	 * @return copies of Armstrong relation
	 */
	public static List<List<String>> createCopyOfMinArmRel(List<List<String>> minArmRel,int startCopyNum,int endCopyNum){
		List<List<String>> copy = new ArrayList<List<String>>();
		for(int i = startCopyNum;i < endCopyNum;i++ ) {
			for(List<String> tuple : minArmRel) {
				ArrayList<String> t = new ArrayList<String>();
				for(String value : tuple) {
					t.add(value+"_"+i);
				}
				copy.add(t);
			}
		}
		return copy;
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
	 * @param schema
	 * @param para
	 * @param FDCoverType
	 * @param repeat
	 * @param keys
	 * @param FDs
	 * @param tableName
	 * @param insert_row_num_list
	 * @return
	 * @throws SQLException
	 * @throws IOException
	 */
	public static List<Double> runSingleExp(String output, int repeat, Schema3NF schema,List<List<String>> armRel, String tableName,int arm_rel_num, List<Integer> insert_num_list) throws SQLException {
		System.out.println("\n###############################\n");
		List<Double> result = new ArrayList<Double>();
		List<String> uniqueIDs = new ArrayList<>();
		String triggerID = "tri_"+tableName;
		
		//create projection table on database
		DBUtils.createTable("freeman", tableName,schema.getAttr_set());
				
		
//		List<List<String>> armRelCopy = createCopyOfMinArmRel(armRel, 0, copyNum);
		List<List<String>> ArmstrongRelation = SyntheticExp.createSpecificTuplesOfMinArmRel(armRel, arm_rel_num, 0);
				
		//insert copy
		DBUtils.insertData("freeman", tableName,ArmstrongRelation);
		
		//add unique constraints
		System.out.println("add trigger and unique constraints...");
		int uc_id = 0;
		for(Key k : schema.getMin_key_list()) {
			String uc_name = "uc_"+tableName+"_"+uc_id ++;
			uniqueIDs.add(uc_name);
			DBUtils.addUnique("freeman", k,tableName,uc_name);
		}
		
		//add FD triggers if exists
		if(!schema.getFd_set().isEmpty()) {
			DBUtils.addTrigger("freeman", schema.getFd_set(),tableName,triggerID);
		}
		
		
		//update experiment
		for(Integer insert_row_num : insert_num_list) {
			List<Double> cost_list = new ArrayList<Double>();
			List<List<String>> syntheData = createSpecificTuplesOfMinArmRel(armRel, insert_row_num, ArmstrongRelation.size()/armRel.size() + 1);
			for(int i = 0;i < repeat;i ++) {
				double cost = DBUtils.insertData("freeman", tableName,syntheData);
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
		String res = tableName+","+ArmstrongRelation.size()+","+schema.getMin_key_list().size()+","+schema.getFd_set().size()+stat;
		Utils.writeContent(Arrays.asList(res), output, true);
		
		System.out.println(res.replace(",", " | "));
		System.out.println("###############################\n");
		
		return result;
	}
	
	public static void runExps(Schema3NF schema, String output, String tableName, int repeat,int arm_rel_num, List<Integer> insert_num_list) throws SQLException {
//		Schema3NF schema = SyntheticExp.getPSchema(p);//key number: p + 2, FD number: p + 3
		List<String> R = schema.getAttr_set();
		List<Key> Keys = schema.getMin_key_list();//all keys
		List<FD> FDs = schema.getFd_set();//all FDs
		//compute Armstrong relation
//		List<List<String>> armRel = Utils.computeMinArmstrongRelation(R, FDs);
		List<List<String>> armRel = DBUtils.genInsertedDataset(10, R.size());
		
		for(int i = 1;i <= FDs.size();i ++) {
			List<FD> selectedFDs = new ArrayList<FD>();//randomly select i  FDs from all FD set to experiments
			Random rand = new Random();
			List<FD> restFds = new ArrayList<>(FDs);
			for(int j = 0;j < i;j ++) {
				int r = rand.nextInt(restFds.size());
				selectedFDs.add(restFds.remove(r));
			}
			Schema3NF schema3nf = new Schema3NF(R, selectedFDs, Keys);
			SyntheticExp.runSingleExp(output, repeat, schema3nf, armRel, tableName, arm_rel_num, insert_num_list);
		}
	}
		
	public static void runExpsOnVaryingKeysAndFDs(Schema3NF schema, String output, String tableName, int repeat,int arm_rel_num, List<Integer> insert_num_list) throws SQLException {
//		Schema3NF schema = SyntheticExp.getPSchema(p);//key number: p + 2, FD number: p + 3
		List<String> R = schema.getAttr_set();
		List<Key> Keys = schema.getMin_key_list();//all keys
		List<FD> FDs = schema.getFd_set();//all FDs
		//compute Armstrong relation
//		List<List<String>> armRel = Utils.computeMinArmstrongRelation(R, FDs);
		List<List<String>> armRel = DBUtils.genInsertedDataset(10, R.size());
		
		Random rand = new Random();
		for(int m = 1;m <= Keys.size();m ++) {//randomly select m Keys
			for(int i = 1;i <= FDs.size();i ++) {//randomly select i  FDs from all FD set to experiments
				int loop = 0;
				while(loop < repeat) {
					List<Key> selectedKeys = new ArrayList<>();
					List<Key> restKeys = new ArrayList<>(Keys);
					for(int n = 0;n < m;n ++) {
						int r = rand.nextInt(restKeys.size());
						selectedKeys.add(restKeys.remove(r));
					}
					
					List<FD> selectedFDs = new ArrayList<FD>();
					List<FD> restFds = new ArrayList<>(FDs);
					for(int j = 0;j < i;j ++) {
						int r = rand.nextInt(restFds.size());
						selectedFDs.add(restFds.remove(r));
					}
					Schema3NF schema3nf = new Schema3NF(R, selectedFDs, selectedKeys);
					SyntheticExp.runSingleExp(output, 1, schema3nf, armRel, tableName, arm_rel_num, insert_num_list);
					loop ++;
				}
				
			}
		}
	}
	
	public static void main(String[] args) throws SQLException {
		
		//Traffic schema experiments
		int repeat = 5;
		List<Integer> arm_rel_num_list = Arrays.asList(1000,10000,100000);//Armstrong relation number
		List<Integer> insert_row_list = Arrays.asList(1000,2000,3000);
		String output = "";
		//varying Key numbers and FD number
		Schema3NF schema = SyntheticExp.getTrafficSchema();
		for(int arn : arm_rel_num_list) {
			SyntheticExp.runExpsOnVaryingKeysAndFDs(schema, output, "traffic", repeat, arn, insert_row_list);
		}
	}

}
