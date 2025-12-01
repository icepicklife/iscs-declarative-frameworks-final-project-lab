package orm;

import java.lang.reflect.*; 
import java.util.HashMap;

import io.github.lukehutch.fastclasspathscanner.*;

import annotations.*;
import dao.BasicMapper;

public class MyORM 
{	
	
	HashMap<Class<?>, Class<?>> entityToMapperMap = new HashMap<>();
	
	
	public void init() throws Exception
	{
		// scan all mappers -- @MappedClass
		scanMappers();		
		
		// scan all the entities -- @Entity
		scanEntities();
				
		// create all entity tables
		createTables();

	}


	private void scanMappers() throws ClassNotFoundException 
	{
		// use FastClasspathScanner to scan the dao package for @MappedClass
		// check if the clazz has the @Entity annotation
			// if not throw new RuntimeException("No @Entity")
		// map the clazz to the mapper class
		
		new FastClasspathScanner("dao")
			.matchClassesWithAnnotation(MappedClass.class, c -> {
				
				MappedClass mc = c.getAnnotation(MappedClass.class);
                Class<?> entityClass = mc.clazz();

                if (!entityClass.isAnnotationPresent(Entity.class)) {
                    throw new RuntimeException("Entity class " + entityClass.getName() + " missing @Entity");
                }

                entityToMapperMap.put(entityClass, c);
                System.out.println("ORM: Mapped " + entityClass.getSimpleName() + " -> " + c.getSimpleName());
                
			})
			.scan();

	}
	

	private void scanEntities() throws ClassNotFoundException 
	{
		// use FastClasspathScanner to scan the entity package for @Entity
			// go through each of the fields 
			// check if there is only 1 field with a Column id attribute
			// if more than one field has id throw new RuntimeException("duplicate id=true")
		
		new FastClasspathScanner("entity")
		.matchClassesWithAnnotation(MappedClass.class, c -> {
			
			int id_count = 0;
			
			for (Field fielders : c.getDeclaredFields()) {
				
				if (fielders.isAnnotationPresent(Column.class)) {
					
					Column retrieved_column = fielders.getAnnotation(Column.class);
					
					if (retrieved_column.id()) {
						
						id_count++;
					}	
				}
			}
			if (id_count != 1) {
				
				throw new RuntimeException("Entity " + c.getSimpleName() + "must have exactly 1 ID Column.");
				
			}  
		})
		.scan();
		
		
		
	}
	
	
	public Object getMapper(Class clazz)
	{
		// create the proxy object for the mapper class supplied in clazz parameter
		// all proxies will use the supplied DaoInvocationHandler as the InvocationHandler
		
		Class<?> classMapper_interf = entityToMapperMap.get(clazz);
		
		if (classMapper_interf == null) {
			
			throw new RuntimeException("No Mapper interface found for entity: " + clazz.getSimpleName() + ".");
		}
		
		DaoInvocationHandler project_handler = new DaoInvocationHandler();
		
		Object project_proxy = Proxy.newProxyInstance(
								classMapper_interf.getClassLoader(),
								new Class<?>[] { classMapper_interf },
								project_handler);

		return project_proxy;
	}
	

	private void createTables()
	{
		// go through all the Mapper classes in the map
					// create a proxy instance for each
					// all these proxies can be casted to BasicMapper
					// run the createTable() method on each of the proxies
		
		for (Class<?> entityClazzes : entityToMapperMap.keySet()) {
			
			Object table_proxy = getMapper(entityClazzes);
			BasicMapper<?> table_mappers = (BasicMapper<?>) table_proxy;
			
			table_mappers.createTable();
			
			System.out.println("Table created for: " + entityClazzes.getSimpleName());
			
		}
		
		
	}

	

	
	
}
