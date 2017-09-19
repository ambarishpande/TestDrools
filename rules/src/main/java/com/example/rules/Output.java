package com.example.rules;

import java.io.Serializable;

public interface Output extends Serializable
{
  void emit(String str);
}
