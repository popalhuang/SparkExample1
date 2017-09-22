package org.apache.spark.examples;
import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.net.URL;
import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;

import com.sun.research.ws.wadl.Method;

public class Main {

	public static void main(String[] args) {		
		// TODO Auto-generated method stub
		System.out.println(Main.class.getPackage().getName());
		String jarpath = args[0];
		String filterpackage=args[1];
		List<Class> classes = new Main().getClasssFromPackage(Main.class.getPackage().getName(),jarpath);
		System.out.println("classes size:"+classes.size());
		
        for (Class clas :classes) {
        	if(clas.getName().indexOf("Main")< 0 && clas.getName().indexOf("$")<0 && clas.getName().indexOf(filterpackage)>=0){        		
        		java.lang.reflect.Method[] methodarray = clas.getMethods();
        		for(int i=0;i<methodarray.length;i++){
        			if("main".equals(methodarray[i].getName())){
        				System.out.println("run-example "+clas.getName().substring(26,clas.getName().length()));
        				break;
        			}
        			
        		}
        		//System.out.println(clas.getMethod("main",String[]));
        	}
             
        } 
	}
	public List<Class> getClasssFromPackage(String pack,String jarpath) {  
	    List<Class> clazzs = new ArrayList<Class>();  
	  
	    // 是否循环搜索子包  
	    boolean recursive = true;  
	  
	    // 包名字  
	    String packageName = pack;  
	    // 包名对应的路径名称  
	    String packageDirName = packageName.replace('.', '/');  
	  
	    Enumeration<URL> dirs;  
	  
	    try {  
	        dirs = Thread.currentThread().getContextClassLoader().getResources(packageDirName);  
	        while (dirs.hasMoreElements()) {  
	            URL url = dirs.nextElement();  
	  
	            //String protocol = url.getProtocol();
	            String protocol = "jar";
	            System.out.println(protocol); 
	            if ("file".equals(protocol)) {  
	                  
	                String filePath = URLDecoder.decode(url.getFile(), "UTF-8"); 
	                System.out.println("file path:"+filePath);
	                findClassInPackageByFile(packageName, filePath, recursive, clazzs);  
	            } else if ("jar".equals(protocol)) { 	                	                
	                //String filePath = "C://Developer//workspaces//spark_workspace//SparkExample//target//SparkExample-0.0.1.jar";
	            	//String filePath = URLDecoder.decode(url.getFile(), "UTF-8");	            	
	                clazzs = getClasssFromJarFile(jarpath,packageDirName); 
	            }  
	        }  
	  
	    } catch (Exception e) {  
	        e.printStackTrace();  
	    }  
	  
	    return clazzs;  
	}
	
	
	public static List<Class> getClasssFromJarFile(String jarPaht, String filePaht) {  
	    List<Class> clazzs = new ArrayList<Class>();  
	  
	    JarFile jarFile = null;  
	    try {  
	        jarFile = new JarFile(jarPaht);  
	    } catch (IOException e1) {  
	        e1.printStackTrace();  
	    }  
	  
	    List<JarEntry> jarEntryList = new ArrayList<JarEntry>();  
	  
	    Enumeration<JarEntry> ee = jarFile.entries(); 
	    
	    while (ee.hasMoreElements()) { 
	    	
	        JarEntry entry = (JarEntry) ee.nextElement();  
	        // 过滤我们出满足我们需求的东西
	        
	        if (entry.getName().startsWith(filePaht) && entry.getName().endsWith(".class")) {
	        	//System.out.println(entry.getName());
	            jarEntryList.add(entry);  
	        }  
	    }  
	    for (JarEntry entry : jarEntryList) {  
	        String className = entry.getName().replace('/', '.');  
	        className = className.substring(0, className.length() - 6);  
	        
	        try {  
	            clazzs.add(Thread.currentThread().getContextClassLoader().loadClass(className));  
	        } catch (ClassNotFoundException e) {  
	            e.printStackTrace();  
	        }  
	    }
	    return clazzs;
	}
	public static void findClassInPackageByFile(String packageName, String filePath, final boolean recursive, List<Class> clazzs) {  
	    File dir = new File(filePath);  
	    if (!dir.exists() || !dir.isDirectory()) {  
	        return;  
	    }  
	    // 在给定的目录下找到所有的文件，并且进行条件过滤  
	    File[] dirFiles = dir.listFiles(new FileFilter() {  
	  
	        @Override  
	        public boolean accept(File file) {  
	            boolean acceptDir = recursive && file.isDirectory();// 接受dir目录  
	            boolean acceptClass = file.getName().endsWith("class");// 接受class文件  
	            return acceptDir || acceptClass;  
	        }  
	    });  
	  
	    for (File file : dirFiles) {  
	        if (file.isDirectory()) {  
	            findClassInPackageByFile(packageName + "." + file.getName(), file.getAbsolutePath(), recursive, clazzs);  
	        } else {  
	            String className = file.getName().substring(0, file.getName().length() - 6);  
	            try {  
	                clazzs.add(Thread.currentThread().getContextClassLoader().loadClass(packageName + "." + className));  
	            } catch (Exception e) {  
	                e.printStackTrace();  
	            }  
	        }  
	    }  
	}  
}
