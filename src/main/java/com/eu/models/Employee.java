package com.eu.models;

import com.google.common.base.MoreObjects;

import java.io.Serializable;

/**
 * Created by ronyjohn on 09/04/17.
 */
public class Employee implements Serializable {
    private long id;

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getAddress() {
        return address;
    }

    public void setAddress(String address) {
        this.address = address;
    }

    private String name;
    private String address;

    public String toString() {
        return MoreObjects.toStringHelper(this).add("id", id).toString();
    }
}
