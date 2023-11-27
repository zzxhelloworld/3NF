package exp;

import java.util.Arrays;
import java.util.List;

import object.DataTypeEnum;
import object.Dataset;
import object.FD;
import object.Parameter;
import util.Utils;
import nf.*;

public class RunningExample {
	
	public static void main(String[] args) throws Exception {
		List<String> R = Arrays.asList("A","B","C","D","E");
		FD fd1 = new FD(Arrays.asList("A","B","D"), Arrays.asList("E"));//ABD->E
		FD fd2 = new FD(Arrays.asList("B","D","E"), Arrays.asList("A"));//BDE->A
		FD fd3 = new FD(Arrays.asList("B","C","D"), Arrays.asList("A"));//BCD->A
		FD fd4 = new FD(Arrays.asList("A","B"), Arrays.asList("C"));//AB->C
		FD fd5 = new FD(Arrays.asList("B","C","D"), Arrays.asList("E"));//BCD->E
		FD fd6 = new FD(Arrays.asList("E","C"), Arrays.asList("D"));//EC->D
		FD fd7 = new FD(Arrays.asList("E","D"), Arrays.asList("C"));//ED->C
		List<FD> fd_list = Arrays.asList(fd1,fd2,fd3,fd4,fd5,fd6,fd7);
		
		fd_list = Utils.getAtomicClosure(fd_list);
		
//		iCONF iconf = new iCONF();
//		iconf.decomp_and_output("FDN", new Parameter(new Dataset("aa", 13, 96388, ",", null, DataTypeEnum.NULL_UNCERTAINTY)), "reduced minimal", R, fd_list);
		
		CONF conf = new CONF();
		conf.decomp_and_output("FDN", new Parameter(new Dataset("aa", 13, 96388, ",", null, DataTypeEnum.NULL_UNCERTAINTY)), "original", R, fd_list);
		
//		iCONFOpt_minf_maxk minf_maxk = new iCONFOpt_minf_maxk();
//		minf_maxk.decomp_and_output("FDN", new Parameter(new Dataset("aa", 13, 96388, ",", null, DataTypeEnum.NULL_UNCERTAINTY)), "optimal keyfd", R, fd_list);
		
//		iCONFOpt_maxk_minf maxk_minf = new iCONFOpt_maxk_minf();
//		maxk_minf.decomp_and_output("FDN", new Parameter(new Dataset("aa", 13, 96388, ",", null, DataTypeEnum.NULL_UNCERTAINTY)), "optimal keyfd", R, fd_list);
	
//		for(List<String> line : Utils.computeMinArmstrongRelation(R, fd_list)){
//			System.out.println(line);
//		}
		
		
	}

}
