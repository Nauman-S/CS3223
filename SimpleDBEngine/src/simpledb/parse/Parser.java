package simpledb.parse;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import simpledb.index.IndexType;
import simpledb.materialize.AggregationFn;
import simpledb.query.AggregateField;
import simpledb.query.Constant;
import simpledb.query.Expression;
import simpledb.query.Field;
import simpledb.query.Operator;
import simpledb.query.OrderPair;
import simpledb.query.Predicate;
import simpledb.query.Term;
import simpledb.record.Schema;

/**
 * The SimpleDB parser.
 * 
 * @author Edward Sciore
 */
public class Parser {
	private Lexer lex;

	public Parser(String s) {
		lex = new Lexer(s);
	}

// Methods for parsing predicates, terms, expressions, constants, fields and operators

	public String field() {
		return lex.eatId();
	}

	public Field selectField() {
		return lex.eatField();
	}

	public Constant constant() {
		if (lex.matchStringConstant())
			return new Constant(lex.eatStringConstant());
		else
			return new Constant(lex.eatIntConstant());
	}

	public Expression expression() {
		if (lex.matchId())
			return new Expression(field());
		else
			return new Expression(constant());
	}

	public Term term() {
		Expression lhs = expression();
		Operator operator = lex.eatOpr();
		Expression rhs = expression();
		return new Term(lhs, rhs, operator);
	}

	public Predicate predicate() {
		Predicate pred = new Predicate(term());
		if (lex.matchKeyword("and")) {
			lex.eatKeyword("and");
			pred.conjoinWith(predicate());
		}
		return pred;
	}

// Methods for parsing queries

	public QueryData query() {
		lex.eatKeyword("select");

		boolean isDistinct = false;
		if (lex.matchKeyword("distinct")) {
			lex.eatKeyword("distinct");
			isDistinct = true;
		}

		List<Field> fields = selectList();
		List<String> fldnames = new ArrayList<>();

		List<AggregationFn> aggfns = new ArrayList<>();
		for (Field field : fields) {
			if (field.isAggregated()) {
				aggfns.add(((AggregateField) field).getAggregationFunction());
			} else {
				fldnames.add(field.getFldname());
			}
		}
		lex.eatKeyword("from");
		Collection<String> tables = tableList();

		Predicate pred = getPredicate();

		List<String> groupByList = getGroupByList();

		List<OrderPair> orderPairList = getOrderPairList();

		QueryData qd = new QueryData(isDistinct, fldnames, aggfns, tables, pred, groupByList, orderPairList);
		if (!isValidQuery(qd)) {
			throw new BadSyntaxException();
		}
		return qd;
	}

	private boolean isValidQuery(QueryData qd) {
		if (lex.hasNextToken()) {
			return false;
		}
		if (!qd.aggfns().isEmpty() && qd.groupByList().isEmpty() && !qd.fields().isEmpty()) {
			return false;
		}
		return true;
	}

	private List<OrderPair> orderList() {
		List<OrderPair> L = new ArrayList<>();
		String field = field();
		boolean isAsc = true;
		if (lex.matchKeyword("asc")) {
			lex.eatKeyword("asc");
		} else if (lex.matchKeyword("desc")) {
			isAsc = false;
			lex.eatKeyword("desc");
		}

		OrderPair orderPair = new OrderPair(field, isAsc);
		L.add(orderPair);
		if (lex.matchDelim(',')) {
			lex.eatDelim(',');
			L.addAll(orderList());
		}
		return L;
	}

	private Predicate getPredicate() {
		Predicate pred = new Predicate();
		if (lex.matchKeyword("where")) {
			lex.eatKeyword("where");
			pred = predicate();
		}
		return pred;
	}

	private List<String> getGroupByList() {
		List<String> groupByList = new ArrayList<>();
		if (lex.matchKeyword("group")) {
			lex.eatKeyword("group");
			if (lex.matchKeyword("by")) {
				lex.eatKeyword("by");
				groupByList = selectGroup();
			} else {
				throw new BadSyntaxException();
			}
		}
		return groupByList;
	}

	private List<OrderPair> getOrderPairList() {
		List<OrderPair> orderPairList = new ArrayList<>();
		if (lex.matchKeyword("order")) {
			lex.eatKeyword("order");
			if (lex.matchKeyword("by")) {
				lex.eatKeyword("by");
				orderPairList = orderList();
			} else {
				throw new BadSyntaxException();
			}
		}
		return orderPairList;
	}

	private List<Field> selectList() {
		List<Field> L = new ArrayList<>();
		L.add(selectField());
		if (lex.matchDelim(',')) {
			lex.eatDelim(',');
			L.addAll(selectList());
		}
		return L;
	}

	private List<String> selectGroup() {
		List<String> L = new ArrayList<String>();
		L.add(field());
		if (lex.matchDelim(',')) {
			lex.eatDelim(',');
			L.addAll(selectGroup());
		}
		return L;
	}

	private Collection<String> tableList() {
		Collection<String> L = new ArrayList<String>();
		L.add(lex.eatId());
		if (lex.matchDelim(',')) {
			lex.eatDelim(',');
			L.addAll(tableList());
		}
		return L;
	}

// Methods for parsing the various update commands

	public Object updateCmd() {
		if (lex.matchKeyword("insert"))
			return insert();
		else if (lex.matchKeyword("delete"))
			return delete();
		else if (lex.matchKeyword("update"))
			return modify();
		else
			return create();
	}

	private Object create() {
		lex.eatKeyword("create");
		if (lex.matchKeyword("table"))
			return createTable();
		else if (lex.matchKeyword("view"))
			return createView();
		else
			return createIndex();
	}

// Method for parsing delete commands

	public DeleteData delete() {
		lex.eatKeyword("delete");
		lex.eatKeyword("from");
		String tblname = lex.eatId();
		Predicate pred = new Predicate();
		if (lex.matchKeyword("where")) {
			lex.eatKeyword("where");
			pred = predicate();
		}
		return new DeleteData(tblname, pred);
	}

// Methods for parsing insert commands

	public InsertData insert() {
		lex.eatKeyword("insert");
		lex.eatKeyword("into");
		String tblname = lex.eatId();
		lex.eatDelim('(');
		List<String> flds = fieldList();
		lex.eatDelim(')');
		lex.eatKeyword("values");
		lex.eatDelim('(');
		List<Constant> vals = constList();
		lex.eatDelim(')');
		return new InsertData(tblname, flds, vals);
	}

	private List<String> fieldList() {
		List<String> L = new ArrayList<String>();
		L.add(field());
		if (lex.matchDelim(',')) {
			lex.eatDelim(',');
			L.addAll(fieldList());
		}
		return L;
	}

	private List<Constant> constList() {
		List<Constant> L = new ArrayList<Constant>();
		L.add(constant());
		if (lex.matchDelim(',')) {
			lex.eatDelim(',');
			L.addAll(constList());
		}
		return L;
	}

// Method for parsing modify commands

	public ModifyData modify() {
		lex.eatKeyword("update");
		String tblname = lex.eatId();
		lex.eatKeyword("set");
		String fldname = field();
		lex.eatDelim('=');
		Expression newval = expression();
		Predicate pred = new Predicate();
		if (lex.matchKeyword("where")) {
			lex.eatKeyword("where");
			pred = predicate();
		}
		return new ModifyData(tblname, fldname, newval, pred);
	}

// Method for parsing create table commands

	public CreateTableData createTable() {
		lex.eatKeyword("table");
		String tblname = lex.eatId();
		lex.eatDelim('(');
		Schema sch = fieldDefs();
		lex.eatDelim(')');
		return new CreateTableData(tblname, sch);
	}

	private Schema fieldDefs() {
		Schema schema = fieldDef();
		if (lex.matchDelim(',')) {
			lex.eatDelim(',');
			Schema schema2 = fieldDefs();
			schema.addAll(schema2);
		}
		return schema;
	}

	private Schema fieldDef() {
		String fldname = field();
		return fieldType(fldname);
	}

	private Schema fieldType(String fldname) {
		Schema schema = new Schema();
		if (lex.matchKeyword("int")) {
			lex.eatKeyword("int");
			schema.addIntField(fldname);
		} else {
			lex.eatKeyword("varchar");
			lex.eatDelim('(');
			int strLen = lex.eatIntConstant();
			lex.eatDelim(')');
			schema.addStringField(fldname, strLen);
		}
		return schema;
	}

// Method for parsing create view commands

	public CreateViewData createView() {
		lex.eatKeyword("view");
		String viewname = lex.eatId();
		lex.eatKeyword("as");
		QueryData qd = query();
		return new CreateViewData(viewname, qd);
	}

//  Method for parsing create index commands

	public CreateIndexData createIndex() {
		lex.eatKeyword("index");
		String idxname = lex.eatId();
		lex.eatKeyword("on");
		String tblname = lex.eatId();
		lex.eatDelim('(');
		String fldname = field();
		lex.eatDelim(')');

		if (lex.matchKeyword("using")) {
			lex.eatKeyword("using");
			IndexType idxType = lex.eatIdxType();
			return new CreateIndexData(idxname, tblname, idxType, fldname);
		}
		return new CreateIndexData(idxname, tblname, fldname);
	}
}
