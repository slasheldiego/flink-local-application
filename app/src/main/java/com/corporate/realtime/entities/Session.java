
package com.belcorp.realtime.entities; 

import java.io.IOException;
import java.util.Date;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

public class Session{

    private final static ObjectMapper JSON = new ObjectMapper();
    static {
        JSON.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    }

    private Long id;
    private Date date;
    private int quantity;
    private double unitPrice;
    private Long customerId;
    private String country;
    private String description;

    public Session(Long id, 
                Date date, 
                int quantity, 
                double unitPrice,
                Long customerId,
                String country,
                String description){
        this.id = id;
        this.date = date;
        this.quantity = quantity;
        this.unitPrice = unitPrice;
        this.customerId = customerId;
        this.country = country;
        this.description = description;
    }

    public void setId(Long id){
        this.id = id;
    }

    public Long getId(){
        return this.id;
    }

    public void setDate(Date date){
        this.date = date;
    }

    public Date getDate(){
        return this.date;
    }

    public void setQuantity(int quantity){
        this.quantity = quantity;
    }

    public int getQuantity(){
        return this.quantity;
    }

    public void setUnitPrice(double unitPrice){
        this.unitPrice = unitPrice;
    }

    public Double getUnitPrice(){
        return this.unitPrice;
    }

    public void setCustomerId(Long customerId){
        this.customerId = customerId;
    }

    public Long getCustomerId(){
        return this.customerId;
    }

    public void setCountry(String country){
        this.country = country;
    }

    public String getCountry(){
        return this.country;
    }

    public void setDescription(String description){
        this.description = description;
    }

    public String getDescription(){
        return this.description;
    }

    public byte[] toJsonAsBytes() {
        try {
            return JSON.writeValueAsBytes(this);
        } catch (IOException e) {
            return null;
        }
    }

    public static Session fromJsonAsBytes(byte[] bytes) {
        try {
            return JSON.readValue(bytes, Session.class);
        } catch (IOException e) {
            return null;
        }
    }

    @Override
    public String toString() {
        return String.format("ID %d: %s %d %d %d",
                id, date, quantity, unitPrice, customerId);
    }
}