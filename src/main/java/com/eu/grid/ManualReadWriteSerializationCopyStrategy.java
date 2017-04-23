package com.eu.grid;

import com.eu.models.Employee;
import net.sf.ehcache.Element;
import net.sf.ehcache.store.compound.ReadWriteSerializationCopyStrategy;

/**
 * Created by ronyjohn on 16/04/17.
 */
public class ManualReadWriteSerializationCopyStrategy extends ReadWriteSerializationCopyStrategy {

    public Element copyForWrite(Element value, ClassLoader loader) {
        if (value == null) {
            return null;
        } else {

            if (value.getObjectValue() == null) {
                return duplicateElementWithNewValue(value, null);
            }
            return duplicateElementWithNewValue(value, createClone((Employee) value.getObjectValue()));
        }
    }

    public Object createClone(final Employee employee) {
        final Employee clonedEmployee = new Employee();
        clonedEmployee.setId(employee.getId());
        clonedEmployee.setName(employee.getName());
        clonedEmployee.setAddress(employee.getAddress());
        return clonedEmployee;
    }

    public Element copyForRead(Element storedValue, ClassLoader loader) {
        if (storedValue == null) {
            return null;
        } else {
            if (storedValue.getObjectValue() == null) {
                return duplicateElementWithNewValue(storedValue, null);
            }
            return duplicateElementWithNewValue(storedValue, createClone((Employee) storedValue.getObjectValue()));

        }
    }
}
