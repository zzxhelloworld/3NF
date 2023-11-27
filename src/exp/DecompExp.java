package exp;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import nf.CONF;
import nf.DecompAlg2;
import nf.DecompAlg3;
import nf.DecompAlg4;
import nf.iCONF;
import nf.iCONFOpt_minf_maxk;
import nf.iCONFOpt_minf_mink;
import nf.iCONFOpt_maxk_minf;
import object.DataTypeEnum;
import object.Dataset;
import object.FD;
import object.Parameter;
import util.Utils;

/**
 * we execute some decomposition algorithms here, including
 * # iCONFOpt_minf_maxk
 * # iCONFOpt_minf_mink
 * # iCONFOpt_maxk_minf
 * # iCONF
 * # CONF
 * # DecompAlg2(lossless & FD-preserving BCNF decomposition)
 * # DecompAlg3(lossless & FD-preserving 3NF decomposition that removing first redundant schemata with more keys)
 * # DecompAlg4(lossless & FD-preserving 3NF decomposition)
 */
public class DecompExp {
	public static void runExp(String optimTarget, Parameter para) throws Exception {
		List<String> R = Utils.getR(para);
		List<FD> atomic = Utils.computeAtomicCover(para.fd_add);
		//iCONFOpt_minf_maxk
		for(String coverType : Arrays.asList("reduced minimal keyfd")) {
			iCONFOpt_minf_maxk opt_minf_maxk = new iCONFOpt_minf_maxk();
			opt_minf_maxk.decomp_and_output(optimTarget, para, coverType, R, atomic);
		}
		//iCONFOpt_minf_mink
		for(String coverType : Arrays.asList("reduced minimal keyfd")) {
			iCONFOpt_minf_mink opt_minf_mink = new iCONFOpt_minf_mink();
			opt_minf_mink.decomp_and_output(optimTarget, para, coverType, R, atomic);
		}
		//iCONFOpt_maxk_minf
		for(String coverType : Arrays.asList("reduced minimal keyfd")) {
			iCONFOpt_maxk_minf opt_maxk_minf = new iCONFOpt_maxk_minf();
			opt_maxk_minf.decomp_and_output(optimTarget, para, coverType, R, atomic);
		}
		//iCONF
		List<String> coverTypeList = null;
		boolean optimal = true;//check if we can compute optimal cover for sub-schemata
		for(FD fd : atomic) {
			if((fd.getLeftHand().size() + fd.getRightHand().size()) > 17) {
				optimal = false;
				break;
			}
		}
		if(optimal)
			coverTypeList = Arrays.asList("reduced minimal", "reduced minimal keyfd");
		else
			coverTypeList = Arrays.asList("reduced minimal", "reduced minimal keyfd");
		for(String coverType : coverTypeList) {
			iCONF iconf = new iCONF();
			iconf.decomp_and_output(optimTarget, para, coverType, R, atomic);
		}
		//CONF
		CONF conf = new CONF();
		conf.decomp_and_output(optimTarget, para, "original", R, Utils.computeAtomicCover(para.fd_add));
		//CONF-mix
		CONF confMix = new CONF();
		confMix.decomp_and_output(optimTarget, para, "original keyfd", R, Utils.computeAtomicCover(para.fd_add));
		//DecompAlg2
		DecompAlg2 alg2 = new DecompAlg2();
		alg2.decomp_and_output(optimTarget, para, "original", R, Utils.computeAtomicCover(para.fd_add));
		//DecompAlg2-mix
		DecompAlg2 alg2Mix = new DecompAlg2();
		alg2Mix.decomp_and_output(optimTarget, para, "original keyfd", R, Utils.computeAtomicCover(para.fd_add));
		//DecompAlg3
		DecompAlg3 alg3 = new DecompAlg3();
		alg3.decomp_and_output(optimTarget, para, "original", R, Utils.computeAtomicCover(para.fd_add));
		//DecompAlg3-mix
		DecompAlg3 alg3Mix = new DecompAlg3();
		alg3Mix.decomp_and_output(optimTarget, para, "original keyfd", R, Utils.computeAtomicCover(para.fd_add));
		//DecompAlg4
		DecompAlg4 alg4 = new DecompAlg4();
		alg4.decomp_and_output(optimTarget, para, "original", R, Utils.computeAtomicCover(para.fd_add));
		//DecompAlg4-mix
		DecompAlg4 alg4Mix = new DecompAlg4();
		alg4Mix.decomp_and_output(optimTarget, para, "original keyfd", R, Utils.computeAtomicCover(para.fd_add));
	}
	

	public static void main(String[] args) throws Exception {
		String optimTarget = "FDN";//optimization target: FD number(FDN)/FD attribute symbol number(ASN)
		Parameter para = new Parameter(new Dataset(args[0], 30, 512000, ",", "\"\"", DataTypeEnum.NULL_UNCERTAINTY), args[1]);
		DecompExp.runExp(optimTarget, para);
	}

}
