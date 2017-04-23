package com.eu.grid;

import com.eu.models.Employee;
import net.sf.ehcache.Element;
import net.sf.ehcache.store.compound.ReadWriteSerializationCopyStrategy;
import org.nustaq.serialization.FSTConfiguration;

/**
 * Created by ronyjohn on 16/04/17.
 */
public class FSTReadWriteSerializationCopyStrategy extends ReadWriteSerializationCopyStrategy {
    static FSTConfiguration conf = FSTConfiguration.createDefaultConfiguration();

    static {
        conf.registerClass(Employee.class);
    }

    public Element copyForWrite(Element value, ClassLoader loader) {
        if (value == null) {
            return null;
        } else {

            if (value.getObjectValue() == null) {
                return duplicateElementWithNewValue(value, null);
            }
            final byte[] bytes = conf.asByteArray(value.getObjectValue());


            return duplicateElementWithNewValue(value, bytes);
        }
    }

    public Element copyForRead(Element storedValue, ClassLoader loader) {
        if (storedValue == null) {
            return null;
        } else {
            if (storedValue.getObjectValue() == null) {
                return duplicateElementWithNewValue(storedValue, null);
            }
            final byte[] bytes = (byte[]) storedValue.getObjectValue();
            return duplicateElementWithNewValue(storedValue, conf.asObject(bytes));

        }
    }
}
