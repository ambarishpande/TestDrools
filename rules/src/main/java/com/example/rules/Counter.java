package com.example.rules;

import java.io.Serializable;

import com.esotericsoftware.kryo.DefaultSerializer;
import com.esotericsoftware.kryo.serializers.JavaSerializer;

/**
 * Created by pramod on 5/26/17.
 */
@DefaultSerializer(JavaSerializer.class)
public class Counter implements Serializable
{
  int count;

  public int getCount()
  {
    return count;
  }

  public void setCount(int count)
  {
    this.count = count;
  }
}
