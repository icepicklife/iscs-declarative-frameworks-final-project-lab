package orm;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
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
		
		String columnsString = "";
	    String valuesString = "";

	    Field[] fields = entityClass.getDeclaredFields();

	    for (Field f : fields) {
	        Column c = f.getAnnotation(Column.class);


	        f.setAccessible(true);
	        Object value = f.get(o);
	        
	        if (c.id() && value == null) {
	            continue;
	        }

	        columnsString += c.name() + ", ";

	        valuesString += getValueAsSql(value) + ", ";
	    }
	    
	    if (columnsString.endsWith(", ")) {
	        columnsString = columnsString.substring(0, columnsString.length() - 2);
	    }

	    if (valuesString.endsWith(", ")) {
	        valuesString = valuesString.substring(0, valuesString.length() - 2);
	    }
	    
	    String sql = "INSERT INTO " + tableName + " (" + columnsString + ") VALUES (" + valuesString + ")";

	    System.out.println("Executing SQL: " + sql);

	    jdbc.runSQL(sql);
	    
	}

	private void update(Object o, Class entityClass, String tableName) throws IllegalAccessException, Exception {
	    String setPart = "";
	    Field pkField = null;
	    String pkColumnName = null;
	    Object pkValue = null;

	    for (Field f : entityClass.getDeclaredFields()) {
	        if (f.isAnnotationPresent(Column.class)) {
	            Column c = f.getAnnotation(Column.class);
	            f.setAccessible(true);
	            Object value = f.get(o);

	            if (c.id()) {
	                pkField = f;
	                pkColumnName = c.name();
	                pkValue = value;
	            } else {
	                setPart += c.name() + " = " + getValueAsSql(value) + ", ";
	            }
	        }
	    }

	    if (pkField == null) {
	        throw new RuntimeException("No primary key field found in " + entityClass.getName());
	    }
	    if (pkValue == null) {
	        throw new RuntimeException("Primary key value is null for update");
	    }

	    if (setPart.endsWith(", ")) {
	        setPart = setPart.substring(0, setPart.length() - 2);
	    }

	    String sql = "UPDATE " + tableName + " SET " + setPart + " WHERE " + pkColumnName + " = " + getValueAsSql(pkValue);

	    System.out.println("Executing UPDATE SQL: " + sql);

	    jdbc.runSQL(sql);
	}
	
	//handles @select
	private Object select(Method method, Object[] args) throws Exception {

	    MappedClass mapped = method.getAnnotation(MappedClass.class);
	    if (mapped == null) {
	        throw new RuntimeException("@MappedClass annotation missing");
	    }
	    Class<?> clazz = mapped.clazz();

	    String tableName = clazz.getSimpleName();

	    Select selectAnn = method.getAnnotation(Select.class);
	    if (selectAnn == null) {
	        throw new RuntimeException("@Select annotation missing");
	    }
	    String sqlString = selectAnn.value().replace(":table", tableName);

	    java.lang.reflect.Parameter[] params = method.getParameters();
	    for (int i = 0; i < params.length; i++) {
	        java.lang.reflect.Parameter p = params[i];
	        if (p.isAnnotationPresent(Param.class)) {
	            String paramName = p.getAnnotation(Param.class).value();
	            Object paramValue = args[i];
	            if (paramValue instanceof String) {
	                sqlString = sqlString.replace(":" + paramName, "\"" + paramValue + "\"");
	            } else {
	                sqlString = sqlString.replace(":" + paramName, paramValue.toString());
	            }
	        }
	    }

	    List<HashMap<String, Object>> results = jdbc.runSQLQuery(sqlString);

	    if (method.getReturnType() == List.class) {
	        List<Object> returnValue = new ArrayList<>();
	        for (HashMap<String, Object> result : results) {
	            Object o = clazz.getDeclaredConstructor().newInstance();

	            for (String columnName : result.keySet()) {
	                for (Field f : clazz.getDeclaredFields()) {
	                    if (f.isAnnotationPresent(Column.class)) {
	                        Column col = f.getAnnotation(Column.class);
	                        if (col.name().equals(columnName)) {
	                            f.setAccessible(true);
	                            f.set(o, result.get(columnName));
	                        }
	                    }
	                }
	            }

	            returnValue.add(o);
	        }
	        return returnValue;
	    } else {
	        if (results.isEmpty()) {
	            return null;
	        }

	        HashMap<String, Object> result = results.get(0);
	        Object o = clazz.getDeclaredConstructor().newInstance();

	        for (String columnName : result.keySet()) {
	            for (Field f : clazz.getDeclaredFields()) {
	                if (f.isAnnotationPresent(Column.class)) {
	                    Column col = f.getAnnotation(Column.class);
	                    if (col.name().equals(columnName)) {
	                        f.setAccessible(true);
	                        f.set(o, result.get(columnName));
	                    }
	                }
	            }
	        }

	        return o;
	    }
	}
	
}
