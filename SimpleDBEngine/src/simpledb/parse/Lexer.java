package simpledb.parse;

import java.util.*;

import simpledb.index.IndexType;
import simpledb.query.AggregateField;
import simpledb.query.AggregateType;
import simpledb.query.Field;
import java.io.*;

/**
 * The lexical analyzer.
 * 
 * @author Edward Sciore
 */
public class Lexer {
	private Collection<String> keywords;
	private Collection<Character> operatorChars;
	private Collection<String> operators;
	private Collection<String> aggregate;
	private StreamTokenizer tok;

	/**
	 * Creates a new lexical analyzer for SQL statement s.
	 * 
	 * @param s the SQL statement
	 */
	public Lexer(String s) {
		initKeywords();
		initOperators();
		initAggregate();
		tok = new StreamTokenizer(new StringReader(s));
		tok.ordinaryChar('.'); // disallow "." in identifiers
		tok.wordChars('_', '_'); // allow "_" in identifiers
		tok.lowerCaseMode(true); // ids and keywords are converted
		nextToken();
	}

//Methods to check the status of the current token

	/**
	 * Returns true if the current token is the specified delimiter character.
	 * 
	 * @param d a character denoting the delimiter
	 * @return true if the delimiter is the current token
	 */
	public boolean matchDelim(char d) {
		return d == (char) tok.ttype;
	}

	/**
	 * Returns true if the current token is an integer.
	 * 
	 * @return true if the current token is an integer
	 */
	public boolean matchIntConstant() {
		return tok.ttype == StreamTokenizer.TT_NUMBER;
	}

	/**
	 * Returns true if the current token is a string.
	 * 
	 * @return true if the current token is a string
	 */
	public boolean matchStringConstant() {
		return '\'' == (char) tok.ttype;
	}

	/**
	 * Returns true if the current token is the specified keyword.
	 * 
	 * @param w the keyword string
	 * @return true if that keyword is the current token
	 */
	public boolean matchKeyword(String w) {
		return tok.ttype == StreamTokenizer.TT_WORD && tok.sval.equals(w);
	}

	public boolean matchAggregate() {
		return tok.ttype == StreamTokenizer.TT_WORD && aggregate.contains(tok.sval);
	}

	/**
	 * Returns true if the current token is a legal identifier.
	 * 
	 * @return true if the current token is an identifier
	 */
	public boolean matchId() {
		return tok.ttype == StreamTokenizer.TT_WORD && !keywords.contains(tok.sval);
	}

	/**
	 * Returns true if the current token is an operator.
	 * 
	 * @return true if the current token is an operator
	 */
	public boolean matchOperatorChar() {
		return operatorChars.contains((char) tok.ttype);
	}

	/**
	 * Returns true if the string is an operator.
	 * 
	 * @return true if the string is an operator
	 */
	public boolean matchOperators(String s) {
		return operators.contains(s);
	}

	/**
	 * Returns true if the current token is a legal index type.
	 * 
	 * @return true if the current token is an index type.
	 */
	public boolean matchIdxType() {
		Collection<String> operators = Arrays.asList(IndexType.HASH.toString().toLowerCase(),
				IndexType.BTREE.toString().toLowerCase());
		return tok.ttype == StreamTokenizer.TT_WORD && operators.contains(tok.sval);
	}

//Methods to "eat" the current token

	/**
	 * Throws an exception if the current token is not the specified delimiter.
	 * Otherwise, moves to the next token.
	 * 
	 * @param d a character denoting the delimiter
	 */
	public void eatDelim(char d) {
		if (!matchDelim(d))
			throw new BadSyntaxException();
		nextToken();
	}

	/**
	 * Throws an exception if the current token is not an integer. Otherwise,
	 * returns that integer and moves to the next token.
	 * 
	 * @return the integer value of the current token
	 */
	public int eatIntConstant() {
		if (!matchIntConstant())
			throw new BadSyntaxException();
		int i = (int) tok.nval;
		nextToken();
		return i;
	}

	/**
	 * Throws an exception if the current token is not a string. Otherwise, returns
	 * that string and moves to the next token.
	 * 
	 * @return the string value of the current token
	 */
	public String eatStringConstant() {
		if (!matchStringConstant())
			throw new BadSyntaxException();
		String s = tok.sval; // constants are not converted to lower case
		nextToken();
		return s;
	}

	public Field eatField() {
		if (matchAggregate()) {
			AggregateField aggregate = eatAggregate();
			return aggregate;
		}

		return new Field(eatId());
	}

	/**
	 * Throws an exception if the current token is not the specified keyword.
	 * Otherwise, moves to the next token.
	 * 
	 * @param w the keyword string
	 */
	public void eatKeyword(String w) {
		if (!matchKeyword(w))
			throw new BadSyntaxException();
		nextToken();
	}

	public AggregateField eatAggregate() {
		AggregateType type = AggregateType.valueOf(tok.sval.toUpperCase());
		nextToken();
		eatDelim('(');
		String fldname = eatId();
		eatDelim(')');
		return new AggregateField(fldname, type);
	}

	/**
	 * Throws an exception if the current token is not an operator. Otherwise,
	 * returns the identifier string and moves to the next token.
	 * 
	 * @return the string value of the current token
	 */
	public String eatOperator() {
		String s1 = (char) tok.ttype + "";
		if (!matchOperatorChar()) { // not an operator
			throw new BadSyntaxException();
		}
		nextToken();

		String s2 = (char) tok.ttype + "";
		if (!matchOperatorChar()) { // check if next token is an operator
			return s1;
		}
		if (!matchOperators(s1 + s2)) { // invalid operator
			throw new BadSyntaxException();
		}
		nextToken();
		return s1 + s2;
	}

	/**
	 * Throws an exception if the current index Type is unidentifiable Otherwise,
	 * moves to the next token.
	 * 
	 * @return the enum of the current index Type identified
	 */
	public IndexType eatIdxType() {
		if (!matchIdxType())
			throw new BadSyntaxException();
		String s = tok.sval;
		nextToken();
		return IndexType.valueOf(s.toUpperCase());
	}

	/**
	 * Throws an exception if the current token is not an identifier. Otherwise,
	 * returns the identifier string and moves to the next token.
	 * 
	 * @return the string value of the current token
	 */
	public String eatId() {
		if (!matchId())
			throw new BadSyntaxException();
		String s = tok.sval;
		nextToken();
		return s;
	}

	public boolean hasNextToken() {
		return tok.ttype != StreamTokenizer.TT_EOF || tok.sval != null;
	}

	private void nextToken() {
		try {
			tok.nextToken();
		} catch (IOException e) {
			throw new BadSyntaxException();
		}
	}

	private void initKeywords() {
		keywords = Arrays.asList("select", "from", "where", "and", "insert", "into", "values", "delete", "update",
				"set", "create", "table", "int", "varchar", "view", "as", "index", "on", "order", "by", "group",
				"explain");
	}

	private void initOperators() {
		operatorChars = Arrays.asList('=', '!', '<', '>');
		operators = Arrays.asList("=", "!=", "<>", "<", "<=", ">", ">=");
	}

	private void initAggregate() {
		aggregate = Arrays.asList("max", "min", "avg", "sum", "count");
	}

}