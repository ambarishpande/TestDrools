package com.example.myapexapp5.app2;

import java.util.Random;

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

  int emitCount = 1000;

  @Override
  public void setup(Context.OperatorContext context)
  {
    random = new Random(System.currentTimeMillis());
  }

  @Override
  public void beginWindow(long windowId)
  {
    emitted = false;
  }

  @Override
  public void emitTuples()
  {
    for (int i = 0; i < emitCount; ++i) {
      Measure measure = new Measure();
      measure.setPayload(new byte[200]);
      measure.setValue(System.currentTimeMillis() % 100 + 50 * random.nextGaussian());
      output.emit(measure);
    }
    try {
      Thread.sleep(100);
    } catch (InterruptedException e) {
      throw Throwables.propagate(e);
    }
  }

  public transient final DefaultOutputPort<Measure> output = new DefaultOutputPort<>();
}
