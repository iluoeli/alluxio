package alluxio.client.file.cache.RL;

public abstract class RLAgent {
	protected int mStep;
	protected double mCurrentReward;
	protected double mCurrentHitRatio;
	private long mAccessSize;
	private long mHitSize;
	State mCurrentState;
	State mLastState;
	Action mCurrentAction;
	double mDelay;

	public void AddCurrentReword(long allSize, long missSize) {
		mAccessSize += allSize;
		mHitSize += (allSize - missSize);
	}

	public void newStep() {
		mStep ++;
		mHitSize = 0;
		mAccessSize = 0;
	}

	protected void simpleCurrentReward() {
		mCurrentHitRatio = ((double)mHitSize / (double)mAccessSize) * 10;
		mHitSize = 0;
		mAccessSize = 0;
	}

	class State {
		double mLowize;
		double mHighSize;
		double mFullSize;
		double mHitRatio;

		public State(double low, double high, double full) {
			mLowize = low;
			mHighSize = high;
			mFullSize = full;
		}

		@Override
		public int hashCode() {
			return (int)(((mLowize * 31 + mHighSize ) * 31 + mFullSize) * 31 + mHitRatio );
		}

		@Override
		public boolean equals(Object obj) {
			if(obj instanceof  State) {
				State o = (State) obj;
				return mLowize == o.mLowize && mHighSize == o.mHighSize &&
					mFullSize == o.mFullSize && mHitRatio == o.mHitRatio;
			}
			return false;
		}
	}

	class Action{
		double mLowChangeSize;
		double mHighChangeSize;
		double mFullChangeSize;
	}

  public void test() {
		simpleCurrentReward();
	}

	public abstract void execute() ;
}
