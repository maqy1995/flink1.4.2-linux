package api;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;

/**
 * A MixedData contains a series of Operator
 */
public class MixedData<T> {
	protected final TypeInformation typeInformation=null;

	/**
	 * Constructor
	 */
	public MixedData(){

	}

	/**
	 * map
	 * @return MixedData
	 */
	public MixedData map(){
		System.out.println("this is MixedData map");
		return null;
	}

	public TypeInformation<T> getType() {
		return typeInformation;
	}

	/**
	 * Xjoin,used to join two MixedData
	 */
	public void Xjoin(){

	}
}
