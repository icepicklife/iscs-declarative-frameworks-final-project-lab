package orm;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

import annotations.*;
import realdb.GhettoJdbcBlackBox;

public class DaoInvocationHandler implements InvocationHandler {

	static GhettoJdbcBlackBox jdbc;
	
	public DaoInvocationHandler() {
		// TODO Auto-generated constructor stub
		
		if (jdbc==null)
		{
			jdbc = new GhettoJdbcBlackBox();
			jdbc.init("com.mysql.cj.jdbc.Driver", 				// DO NOT CHANGE
					  "jdbc:mysql://localhost/studentdb",    // change jdbcblackbox to the DB name you wish to use
					  "root", 									// USER NAME
					  "");										// PASSWORD
		}
	}
	
	@Override
	public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
		
		// determine method annotation type and call the appropriate method
			// @CreateTable
			// @Save
			// @Delete
			// @Select
		
		if (method.isAnnotationPresent(CreateTable.class)) {
	        createTable(method);
	    }
		
		if (method.isAnnotationPresent(Delete.class)) {
	        delete(method, args[0]);
	    }
		
		if (method.isAnnotationPresent(Save.class)) {
	        save(method, args[0]);
	    }

	    if (method.isAnnotationPresent(Select.class)) {
	        return select(method, args);
	    }
			
		return null;
	}
	
	
	// HELPER METHOD: when putting in field values into SQL, strings are in quotes otherwise they go in as is
	private String getValueAsSql(Object o) throws Exception
	{
		if (o.getClass()==String.class)
		{
			return "\""+o+"\"";
		}
		else
		{
			return String.valueOf(o);
		}		
	}
	
	
	// handles @CreateTable
	private void createTable(Method method)
	{
		
// 		SAMPLE SQL 		
//	    CREATE TABLE REGISTRATION (id INTEGER not NULL AUTO_INCREMENT,
//												first VARCHAR(255), 
//												last VARCHAR(255), age INTEGER, PRIMARY KEY ( id ))
		
// 		Using the @MappedClass annotation from method
		// get the required class 		
		// use reflection to check all the fields for @Column
		// use the @Column attributed to generate the required sql statment
		
// 		Run the sql
		// jdbc.runSQL(SQL STRING);
		
		 // Get the entity class from @MappedClass

	    MappedClass mappedClass = method.getAnnotation(MappedClass.class);
	    if (mappedClass == null)
	        throw new RuntimeException("@CreateTable requires @MappedClass on the method");

	    Class<?> clazz = mappedClass.clazz();  // entity class
	    String tableName = clazz.getSimpleName();

	    String template = "CREATE TABLE <name> (<fields> PRIMARY KEY (<id>))";

	    String fieldString = "";
	    String pkColumn = null;

	    Field[] fields = clazz.getDeclaredFields();
	    for (Field f : fields)
	    {
	        if (f.isAnnotationPresent(Column.class))
	        {
	            Column c = f.getAnnotation(Column.class);
	            String name = c.name();
	            String sql = c.sqlType();

	            if (c.id())
	                pkColumn = name;

	            fieldString += name + " " + sql + ", ";
	        }
	    }

	    String returnSql = template
	            .replace("<name>", tableName)
	            .replace("<fields>", fieldString)
	            .replace("<id>", pkColumn);

	    System.out.println("Executing CreateTable SQL: " + returnSql);

	    jdbc.runSQL(returnSql);
	}
	
	// handles @Delete
	private void delete(Method method, Object o) throws Exception
	{
// 		SAMPLE SQL		
//  	DELETE FROM REGISTRATION WHERE ID=1
		
		
// 		Using the @MappedClass annotation from method
		// get the required class 		
		// use reflection to check all the fields for @Column
		// find which field is the primary key
		// for the Object o parameter, get the value of the field and use this as the primary value 
		// for the WHERE clause
				// if the primary key field value is null, throw a RuntimeException("no pk value")
		
		
		// run the sql
//		jdbc.runSQL(SQL STRING);
		
	    MappedClass mapped = method.getAnnotation(MappedClass.class);

	    Class<?> entityClass = mapped.clazz();
	    String tableName = entityClass.getSimpleName();

	    Field pkField = null;
	    String pkColumnName = null;

	    for (Field f : entityClass.getDeclaredFields()) {
	        if (f.isAnnotationPresent(Column.class)) {
	            Column c = f.getAnnotation(Column.class);
	            if (c.id()) {
	                pkField = f;
	                pkColumnName = c.name();
	                break;
	            }
	        }
	    }

	    pkField.setAccessible(true);

	    Object pkValue = pkField.get(o);

	    if (pkValue == null) {
	        throw new RuntimeException("no pk value");
	    }

	    String pkValueSql = getValueAsSql(pkValue);

	    String sql = "DELETE FROM " + tableName + " WHERE " + pkColumnName + "=" + pkValueSql;

	    System.out.println("Executing Delete SQL: " + sql);
	    jdbc.runSQL(sql);
	}
	
	// handles @Save
	private void save(Method method, Object o) throws Exception
	{
// 		Using the @MappedClass annotation from method
		// get the required class 		
		// use reflection to check all the fields for @Column
		// find which field is the primary key
		// for the Object o parameter, get the value of the field
			// if the field is null run the insert(Object o, Class entityClass, String tableName) method
			// if the field is not null run the update(Object o, Class entityClass, String tableName) method
		
	    MappedClass mc = method.getAnnotation(MappedClass.class);
	    if (mc == null)
	        throw new RuntimeException("@MappedClass annotation missing");

	    Class<?> entityClass = mc.clazz();

	    Field primaryKeyField = null;

	    for (Field field : entityClass.getDeclaredFields()) {
	        Column c = field.getAnnotation(Column.class);
	        if (c != null && c.id()) {
	            primaryKeyField = field;
	            break;
	        }
	    }

	    if (primaryKeyField == null)
	        throw new RuntimeException("No primary key field found in " + entityClass.getName());

	    primaryKeyField.setAccessible(true);
	    
	    Object pkValue = primaryKeyField.get(o);

	    String tableName = entityClass.getSimpleName();  

	    if (pkValue == null) {
	        insert(o, entityClass, tableName);
	    } else {
	        update(o, entityClass, tableName);
	    }

	}

	private void insert(Object o, Class entityClass, String tableName) throws Exception 
	{
		
// 		SAMPLE SQL		
//		INSERT INTO table_name (column1, column2, column3, ...)
//		VALUES (value1, value2, value3, ...)	


//		HINT: columnX comes from the entityClass, valueX comes from o 
		
		
// 		run sql		
//		jdbc.runSQL(SQL STRING);
		
		String columnsPart = "";
	    String valuesPart = "";

	    Field[] fields = entityClass.getDeclaredFields();

	    for (Field f : fields) {
	        Column c = f.getAnnotation(Column.class);


	        f.setAccessible(true);
	        Object value = f.get(o);
	        
	        if (c.id() && value == null) {
	            continue;
	        }

	        columnsPart += c.name() + ", ";

	        valuesPart += getValueAsSql(value) + ", ";
	    }
	    
	    if (columnsPart.endsWith(", ")) {
	        columnsPart = columnsPart.substring(0, columnsPart.length() - 2);
	    }

	    if (valuesPart.endsWith(", ")) {
	        valuesPart = valuesPart.substring(0, valuesPart.length() - 2);
	    }
	    
	    String sql = "INSERT INTO " + tableName + " (" + columnsPart + ") VALUES (" + valuesPart + ")";

	    System.out.println("Executing SQL: " + sql);

	    jdbc.runSQL(sql);
	    
	}

	private void update(Object o, Class entityClass, String tableName) throws IllegalAccessException, Exception {

//		SAMPLE SQL		
//		UPDATE table_name
//		SET column1 = value1, column2 = value2, ...
//		WHERE condition;
		
//		HINT: columnX comes from the entityClass, valueX comes from o 		
		
//		run sql
//		jdbc.runSQL(SQL STRING);
	}

		
	// handles @Select
	private Object select(Method method, Object[] args) throws Exception
	{
		// same style as lab
		
// PART I		
// 		Using the @MappedClass annotation from method
//		get the required class
//		Use this class to extra all the column information (this is the replacement for @Results/@Result)		
//		generate the SELECT QUERY		

// PART II
		
//		this will pull actual values from the DB		
//		List<HashMap<String, Object>> results = jdbc.runSQLQuery(SQL QUERY);

		
		// process list based on getReturnType
		if (method.getReturnType()==List.class)
		{
			List returnValue = new ArrayList();
			
			// create an instance for each entry in results based on mapped class
			// map the values to the corresponding fields in the object
			// DO NOT HARD CODE THE TYPE and FIELDS USE REFLECTION
			
			return returnValue;
		}
		else
		{
			// if not a list return type
			
			// if the results.size() == 0 return null
			// if the results.size() >1 throw new RuntimeException("More than one object matches")
			// if the results.size() == 1
				// create one instance based on mapped class
				// map the values to the corresponding fields in the object
				// DO NOT HARD CODE THE TYPE and FIELDS USE REFLECTION
						
			return null;
		}
	}
	
}
