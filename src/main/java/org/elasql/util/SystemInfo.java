package org.elasql.util;

import java.lang.management.ManagementFactory;
//import java.lang.management.OperatingSystemMXBean;
import com.sun.management.OperatingSystemMXBean;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;

public class SystemInfo {
	static long lastSystemTime = -1;
	static long lastCpuTime = -1;
	static double smoothLoad = 0;
    // public static long getCpuUtlity() {
    //     OperatingSystemMXBean operatingSystemMXBean = ManagementFactory.getOperatingSystemMXBean();
    //     for (Method method : operatingSystemMXBean.getClass().getDeclaredMethods()) {
    //         method.setAccessible(true);
    //         if (method.getName().startsWith("getProcessCpuTime") && 
    //             Modifier.isPublic(method.getModifiers())) {
    //             Object value;
    //             try {
    //                 value = method.invoke(operatingSystemMXBean);
    //             } catch (Exception e) {
    //                 value = e;
    //             }
    //             // System.out.println(method.getName() + " = " + value);
    //             return Long.parseLong(value.toString(), 10);
    //         }
    //     }
    //     return 0;
    // }
    public static double getCpuUsage() {
    	// try {
    	// 	OperatingSystemMXBean os = ManagementFactory.getOperatingSystemMXBean();
    	//	if(Class.forName("com.sun.management.OperatingSystemMXBean").isInstance(os)) {
    	//		Method getMemory = os.getClass().getDeclaredMethod("getProcessCpuLoad");
    	//		getMemory.setAccessible(true);
    	//		return (double) getMemory.invoke(os);
    	//	}
    	// }catch (Exception e){
    	//	System.out.print(e);
    	// }
	    OperatingSystemMXBean bean = (com.sun.management.OperatingSystemMXBean) ManagementFactory
	             .getOperatingSystemMXBean();
	    if(lastSystemTime == -1 || lastCpuTime == -1) {
	    	recordTime();
	    	return 0;
	    }
	    long currentSysetemTime = System.nanoTime();
	    long currentCpuTime = bean.getProcessCpuTime();
	    double load = (currentCpuTime - lastCpuTime)/(currentSysetemTime - lastSystemTime);
	    // smoothLoad += (load - smoothLoad) * 0.1;
	    recordTime();
	    return load;
	}
    private static void recordTime() {
    	OperatingSystemMXBean bean = (com.sun.management.OperatingSystemMXBean) ManagementFactory
	             .getOperatingSystemMXBean();
    	lastSystemTime = System.nanoTime();
    	lastCpuTime = bean.getProcessCpuTime();
    }
}