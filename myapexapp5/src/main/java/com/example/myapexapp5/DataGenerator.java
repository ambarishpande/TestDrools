package com.example.myapexapp5;

import java.util.Random;

import com.example.rules.Applicant;
import com.google.common.base.Throwables;
import com.datatorrent.api.Context;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.InputOperator;
import com.datatorrent.common.util.BaseOperator;

/**
 * Created by pramod on 5/26/17.
 */
public class DataGenerator extends BaseOperator implements InputOperator
{

  boolean emitted = false;
  transient Random random;

  @Override
  public void setup(Context.OperatorContext context)
  {
    random = new Random();
  }

  @Override
  public void beginWindow(long windowId)
  {
    emitted = false;
  }

  @Override
  public void emitTuples()
  {
    if (!emitted) {
      Applicant applicant = new Applicant();
      applicant.setName("name" + random.nextInt());
      applicant.setAge(random.nextInt(60));
      output.emit(applicant);
      emitted = true;
    } else {
      try {
        Thread.sleep(10);
      } catch (InterruptedException e) {
        throw Throwables.propagate(e);
      }
    }
  }

  public transient final DefaultOutputPort<Applicant> output = new DefaultOutputPort<>();
}
