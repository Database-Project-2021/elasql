package org.elasql.util;

import java.lang.management.ManagementFactory;
import com.sun.management.OperatingSystemMXBean;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;

public class SystemInfo {
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
	    OperatingSystemMXBean bean = (com.sun.management.OperatingSystemMXBean) ManagementFactory
	            .getOperatingSystemMXBean();

	    return bean.getProcessCpuLoad();
	}
    
}