package com.datatorrent.example.dedupapp;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.Random;

public class AdData implements Serializable
{

  public int getAmount()
  {
    return amount;
  }

  public int getClient_id()
  {
    return client_id;
  }

  public int getSite_id()
  {
    return site_id;
  }

  public int getPage_id()
  {
    return page_id;
  }

  private int client_id, site_id, page_id, amount;
  Random r = new Random();

  public AdData()
  {
    setSiteid();
    setClientid();
    setPageid();
    setAmount();
  }

  private void setAmount()
  {
    amount = r.nextInt(500) + 500;
  }

  private void setPageid()
  {
    page_id = r.nextInt(20);
  }

  private void setClientid()
  {
    client_id = r.nextInt(10);
  }

  public void setSiteid()
  {
    site_id = r.nextInt(20); //value inside the bracket shows the limit
  }

  public String makeKey() // appending required number of zeros to make the total key length 10
  {

    String key = String.format("%05d", this.getSite_id()) + "" + String.format("%05d", this.getPage_id());
    ;
    return key;
  }

  public static byte[] toByteArray(Object obj) throws IOException //converting our object(tuple) into a byte array
  {
    byte[] bytes = null;
    ByteArrayOutputStream bos = null;
    ObjectOutputStream oos = null;
    try {
      bos = new ByteArrayOutputStream();
      oos = new ObjectOutputStream(bos);
      oos.writeObject(obj);
      oos.flush();
      bytes = bos.toByteArray();
    } finally {
      if (oos != null) {
        oos.close();
      }
      if (bos != null) {
        bos.close();
      }
    }
    return bytes;
  }

  public static Object toObject(byte[] bytes) throws IOException, ClassNotFoundException
  { //converting our byte array into an object(tuple)
    Object obj = null;
    ByteArrayInputStream bis = null;
    ObjectInputStream ois = null;
    try {
      bis = new ByteArrayInputStream(bytes);
      ois = new ObjectInputStream(bis);
      obj = ois.readObject();
    } finally {
      if (bis != null) {
        bis.close();
      }
      if (ois != null) {
        ois.close();
      }
    }
    return obj;
  }

  public byte[] makeValue() throws IOException
  { //converts the value(client_id and amount) into byte array
    byte[] bytes = toByteArray(this);

    return bytes;
  }
}


