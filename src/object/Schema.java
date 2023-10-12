package object;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;

import util.Utils;

/**
 * general schema
 */
public class Schema implements SchemaInterface{
	private List<String> attr_set;
	private List<FD> fd_set;
	
	public Schema() {
		this.attr_set = new ArrayList<String>();
		this.fd_set = new ArrayList<FD>();
	}
	
	public Schema(List<String> attr_set,List<FD> fd_set) {
		//sort attr_set if it numeric string
		boolean numeric = true;
		for(String a : attr_set) {
			if(!a.matches("[0-9]+")) {
				numeric = false;
				break;
			}
		}
		if(numeric) {
			attr_set.sort(new Comparator<String>() {
				@Override
				public int compare(String o1, String o2) {
					int num1 = Integer.parseInt(o1);
					int num2 = Integer.parseInt(o2);
					if(num1 > num2)
						return 1;
					else if(num1 < num2)
						return -1;
					else
						return 0;
				}
			});
		}
		
		this.attr_set = attr_set;
		this.fd_set = fd_set;
	}
	
	
	public List<FD> getFd_set() {
		return fd_set;
	}

	public void setFd_set(List<FD> fd_set) {
		this.fd_set = fd_set;
	}


	public List<String> getAttr_set() {
		return attr_set;
	}

	public void setAttr_set(List<String> attr_set) {
		this.attr_set = attr_set;
	}
	
	/**
	 * 
	 * @return true if the schema is BCNF, false otherwise
	 */
	public boolean isBCNF() {
		return Utils.isBCNF(attr_set, fd_set);
	}
	
//	public Schema3NF to3NF() {
//		return new Schema3NF(this.attr_set, this.fd_set);
//	}
	
	/**
	 * convert FDs in 3NF into specific FD/mixed cover
	 * @param coverType
	 * @return
	 */
	public Schema3NF to3NF(String coverType) {
		return new Schema3NF(this.attr_set, Utils.compFDCover(this.fd_set, coverType));
	}
	
	public Schema3NF to3NFWithMixedCover(String coverType) {
		int idx = coverType.indexOf(" keyfd");
		String fdcover1 = coverType.substring(0, idx);//get corresponding fd cover
		List<Object> info = (List<Object>) Utils.compKeyFDCover(this.attr_set, Utils.compFDCover(this.fd_set, fdcover1));
		List<Key> minKeys = (List<Key>) info.get(0);
		List<FD> remainingFDs = (List<FD>) info.get(1);
		remainingFDs = Utils.compFDCover(remainingFDs, fdcover1);
		return new Schema3NF(this.attr_set, remainingFDs, minKeys);
	}
	
	/**
	 * 
	 * @return 3NF if possible, with mixed cover
	 */
//	public Schema3NF to3NFWithMixedCover() {
//		List<Object> info = Utils.compKeyFDCover(attr_set, fd_set);
//		List<Key> minKeys = (List<Key>) info.get(0);
//		List<FD> remainingFDs = (List<FD>) info.get(1);
//		return new Schema3NF(this.attr_set, remainingFDs, minKeys);
//	}
	
	public SchemaBCNF toBCNF() {
		return new SchemaBCNF(this.attr_set, Utils.getMinimalKeys(this.attr_set, this.fd_set));
	}

	@Override
	public int hashCode() {
		return Objects.hash(new HashSet<>(attr_set), new HashSet<>(fd_set));
	}
	
	@Override
	public boolean equals(Object obj) {
		if(obj instanceof Schema) {
			Schema schema = (Schema)obj;
			if(this.attr_set.containsAll(schema.getAttr_set()) && this.attr_set.size() == schema.getAttr_set().size())
				return true;
			else
				return false;
				
		}else
			return false;
	}

	@Override
	public String toString() {
		String output = "Schema: \n";
		output += "Schema: "+attr_set.toString()+"\n";
		output += "FDs: \n";
		for(FD fd : fd_set) {
			output += fd.toString()+"\n";
		}
		output += "**********\n\n";
		return output;
	}

	
	
	
	
}
