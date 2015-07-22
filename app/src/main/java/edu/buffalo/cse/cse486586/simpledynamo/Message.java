package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.Serializable;
import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by chandana on 5/5/15.
 */
public class Message implements Serializable
{
    public String messageType;
    public String mport;
    public ConcurrentHashMap<String,String> result=new ConcurrentHashMap<String,String>();
    public boolean flag;
    public String destport;
    public String count;
    public String key;
    public String value;
}