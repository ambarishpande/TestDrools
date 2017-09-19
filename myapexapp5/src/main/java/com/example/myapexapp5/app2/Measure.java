package com.example.myapexapp5.app2;

public class Measure implements com.example.rules.Measure
{
  double value;
  byte[] payload;

  @Override
  public double getValue()
  {
    return value;
  }

  public void setValue(double value)
  {
    this.value = value;
  }

  public byte[] getPayload()
  {
    return payload;
  }

  public void setPayload(byte[] payload)
  {
    this.payload = payload;
  }

  @Override
  public String toString()
  {
    return "Measure{" +
        "value=" + value +
        '}';
  }
}
