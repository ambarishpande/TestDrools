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

  public int getEmitCount()
  {
    return emitCount;
  }

  public void setEmitCount(int emitCount)
  {
    this.emitCount = emitCount;
  }

  int emitCount;

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
      long currentTime = System.currentTimeMillis();
      measure.setValue((((currentTime % 2) + 1) * random.nextInt((int)(currentTime % 50 + 1))) % 20);
      output.emit(measure);
      System.out.println(measure.getValue());
    }
    try {
      Thread.sleep(100);
    } catch (InterruptedException e) {
      throw Throwables.propagate(e);
    }
  }

  public transient final DefaultOutputPort<Measure> output = new DefaultOutputPort<>();
}
