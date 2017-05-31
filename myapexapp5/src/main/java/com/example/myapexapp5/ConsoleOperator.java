package com.example.myapexapp5;

import com.datatorrent.api.Context;
import com.datatorrent.api.annotation.Stateless;
import com.datatorrent.lib.io.ConsoleOutputOperator;

/**
 * Created by pramod on 5/30/17.
 */
public class ConsoleOperator extends ConsoleOutputOperator
{
  @Override
  public void setup(Context.OperatorContext context)
  {
    super.setup(context);
    if (context.getValue(Context.OperatorContext.ACTIVATION_WINDOW_ID) != Stateless.WINDOW_ID) {
      System.out.println("Restarting operator");
    }
  }
}
