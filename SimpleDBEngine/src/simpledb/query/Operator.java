package simpledb.query;

public class Operator {
	
	int i;
	
	/*A constant indicating <= operator */
	public static final int LESS_THAN_AND_EQUAL_TO = 1;
	
	/*A constant indicating <> or != operator */
	public static final int NOT_EQUAL = 2;
	
	/*A constant indicating < operator */
	public static final int LESS_THAN = 3;
	
	/*A constant indicating >= operator */
	public static final int GREATER_THAN_AND_EQUAL_TO = 4;
	
	/*A constant indicating <= operator */
	public static final int GREATER_THAN = 5;
	
	/*A constant indicating = operator */
	public static final int EQUAL_TO = 6;
	
	
	public Operator(Integer i) {
		this.i = i;
	}
	
	
}
