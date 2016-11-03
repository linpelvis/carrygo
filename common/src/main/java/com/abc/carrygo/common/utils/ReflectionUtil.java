package com.abc.carrygo.common.utils;

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;

/**
 * Created by plin on 10/18/16.
 */
public class ReflectionUtil {

    public static <T> T newInstance(Class<T> clazz, Object... constructorArgs) {
        if (clazz == null) {
            return null;
        }

        T result;
        int argLen = constructorArgs == null ? 0 : constructorArgs.length;

        Class<?>[] parameterTypes = new Class[argLen];
        for (int i = 0; i < argLen; i++) {
            parameterTypes[i] = constructorArgs[i].getClass();
        }

        Constructor<T> constructor = null;

        try {
            constructor = clazz.getDeclaredConstructor(parameterTypes);

            if (!constructor.isAccessible()) {
                constructor.setAccessible(true);
            }

            result = constructor.newInstance(constructorArgs);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        return result;
    }

    public static <T> T invokeMethod(Object object, String methodName, Object... methodArgs) {
        if (object == null) {
            return null;
        }

        T result = null;

        Class<?> clazz = object.getClass();

        int argLen = methodArgs == null ? 0 : methodArgs.length;

        Class<?>[] methodTypes = new Class[argLen];
        for (int i = 0; i < argLen; i++) {
            methodTypes[i] = methodArgs[i].getClass();
        }

        try {
            Method method = clazz.getDeclaredMethod(methodName, methodTypes);

            if (method != null) {
                if (!method.isAccessible()) {
                    method.setAccessible(true);
                }

                result = (T) method.invoke(object, methodArgs);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        return result;
    }
}
