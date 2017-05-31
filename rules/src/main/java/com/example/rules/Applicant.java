package com.example.rules;

/**
 * Created by pramod on 5/26/17.
 */
public class Applicant
{
  String name;
  int age;
  boolean valid;
  int count;

  public Applicant()
  {
    valid = true;
  }

  public String getName()
  {
    return name;
  }

  public void setName(String name)
  {
    this.name = name;
  }

  public int getAge()
  {
    return age;
  }

  public void setAge(int age)
  {
    this.age = age;
  }

  public boolean isValid()
  {
    return valid;
  }

  public void setValid(boolean valid)
  {
    this.valid = valid;
  }

  public int getCount()
  {
    return count;
  }

  public void setCount(int count)
  {
    this.count = count;
  }

  @Override
  public String toString()
  {
    return "Applicant{" +
        "name='" + name + '\'' +
        ", age=" + age +
        ", valid=" + valid +
        ", count=" + count +
        '}';
  }
}
