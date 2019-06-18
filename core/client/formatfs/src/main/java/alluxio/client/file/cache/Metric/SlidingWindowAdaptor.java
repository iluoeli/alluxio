package alluxio.client.file.cache.Metric;

import alluxio.client.file.cache.core.PromotionPolicy;

import java.util.Iterator;
import java.util.LinkedList;

public class SlidingWindowAdaptor {
  protected double mParameterRatio;
  protected double mUpperBound;
  protected double mLowerBound;
  protected double mInterval;
  protected LinkedList<Double> mHitRatioCollector = new LinkedList<>();
  private int triggerNum = 10;
  protected int downNum;
  protected int moveNum = 3;
  private int mFakeUpdateTriggerNum = 3;
  private final PromotionPolicy promotionPolicy;

  public SlidingWindowAdaptor(PromotionPolicy promotionPolicy) {
    this.promotionPolicy = promotionPolicy;
  }


  protected boolean isHitRatioDown() {
    Iterator<Double> iterator = mHitRatioCollector.descendingIterator();
    double base = 0;
    int downNum = 0;
    int upNum = -1;
    while(iterator.hasNext()) {
      double curr = iterator.next();
      if (curr > base) {
        upNum++;
      } else if (curr < base) {
        downNum ++;
      }
      base = curr;
    }
    return downNum > upNum;
  }

  protected boolean isHitRatioUp() {
    Iterator<Double> iterator = mHitRatioCollector.descendingIterator();
    double base = 0;
    int downNum = 0;
    int upNum = -1;
    while(iterator.hasNext()) {
      double curr = iterator.next();
      if (curr > base) {
        upNum++;
      } else if (curr < base) {
        downNum ++;
      }
      base = curr;
    }
    return downNum < upNum;
  }


  private void changeRatio() {
     if (isHitRatioDown()) {
       mParameterRatio =Math.min(mLowerBound, mParameterRatio - mInterval);
       downNum ++;
       if (downNum >= mFakeUpdateTriggerNum) {
         promotionPolicy.setFakeUpdate(true);
       }
     } else if (isHitRatioUp()) {
       mParameterRatio =Math.max(mUpperBound, mParameterRatio + mInterval);
       if (downNum > mFakeUpdateTriggerNum) {
         downNum = 0;
         promotionPolicy.setFakeUpdate(false);
       }
     }
     for (int i = 0 ;i < moveNum; i++) {
       mHitRatioCollector.removeLast();
     }
  }

  public void moveWindow() {
    mHitRatioCollector.addFirst(HitRatioMetric.INSTANCE.getHitRatio());
    if (mHitRatioCollector.size() > triggerNum) {
      changeRatio();
    }
    HitRatioMetric.INSTANCE.accessSize = 0;
    HitRatioMetric.INSTANCE.hitSize = 0;
  }
}
