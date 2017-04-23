package com.eu.grid;

import com.eu.models.Employee;
import com.google.common.base.Throwables;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.marshaller.AbstractNodeNameAwareMarshaller;
import org.jetbrains.annotations.Nullable;
import org.nustaq.serialization.FSTConfiguration;
import org.nustaq.serialization.FSTObjectInput;
import org.nustaq.serialization.FSTObjectOutput;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * Created by ronyjohn on 14/04/17.
 */
public class FstAbstractNodeNameAwareMarshaller extends AbstractNodeNameAwareMarshaller {
    static FSTConfiguration conf = FSTConfiguration.createDefaultConfiguration();

    static {
        conf.registerClass(Employee.class);
    }

    @Override
    protected void marshal0(@Nullable Object o, OutputStream outputStream) throws IgniteCheckedException {
        try (FSTObjectOutput out = new FSTObjectOutput(outputStream);) {
            out.writeObject(o);
        } catch (Exception e) {
            Throwables.throwIfUnchecked(e);
        }

    }

    @Override
    protected byte[] marshal0(@Nullable Object o) throws IgniteCheckedException {
        return conf.asByteArray(o);
    }

    @Override
    protected <T> T unmarshal0(InputStream inputStream, @Nullable ClassLoader classLoader) throws IgniteCheckedException {

        try (FSTObjectInput in = new FSTObjectInput(inputStream);) {
            return (T) in.readObject();
        } catch (IOException e) {
            Throwables.throwIfUnchecked(e);
        } catch (ClassNotFoundException e) {
            Throwables.throwIfUnchecked(e);
        }
        return null;

    }

    @Override
    protected <T> T unmarshal0(byte[] bytes, @Nullable ClassLoader classLoader) throws IgniteCheckedException {
        return (T) conf.asObject(bytes);
    }

    @Override
    public void onUndeploy(ClassLoader classLoader) {

    }
}
