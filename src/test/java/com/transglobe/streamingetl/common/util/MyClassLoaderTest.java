package com.transglobe.streamingetl.common.util;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Enumeration;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;

import javassist.ClassPool;
import javassist.CtClass;
import javassist.LoaderClassPath;
import javassist.NotFoundException;

public class MyClassLoaderTest {

	public static void main(String[] args) {

		//		test1();

		try {
			test2();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (NotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	public static void test2() throws IOException, ClassNotFoundException, NotFoundException {
		//		String pathToJar = "/home/oracle/gitrepository/transglobe/streamingetl-poc/testclient/target/poc-testcient-1.0.jar";
		String pathToJar = "/home/oracle/poc-testcient-1.0.jar";
		JarFile jarFile = null;
		try {
			jarFile = new JarFile(pathToJar);
			Enumeration<JarEntry> e = jarFile.entries();

			URL[] urls = { new URL("jar:file:" + pathToJar+"!/") };
			URLClassLoader cl = URLClassLoader.newInstance(urls);

			ClassPool cp = ClassPool.getDefault();
			cp.appendClassPath(new LoaderClassPath(cl));
			while (e.hasMoreElements()) {
				JarEntry je = e.nextElement();
				if(je.isDirectory() || !je.getName().endsWith(".class")){
					continue;
				}
				// -6 because of .class
				String className = je.getName().substring(0,je.getName().length()-6);
				className = className.replace('/', '.');

				System.out.println("class name:" + className);
				Class c = cl.loadClass(className);

				System.out.println("class :" + c);
				
				CtClass ctClass = cp.get(className);
				
				System.out.println("ctClass :" + ctClass);
				
			}
		} finally {
			if (jarFile != null) jarFile.close();
		}
	}
	public static void test1() {
		Class<?> c;
		try {
			c = Class.forName("com.transglobe.streamingetl.common.util.MyClass");

			Constructor<?> cons = c.getConstructor();

			MyClass object = (MyClass)cons.newInstance();

			System.out.println("object:" + object.getName());



		} catch (ClassNotFoundException | NoSuchMethodException | SecurityException | InstantiationException | IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}


	}
}
