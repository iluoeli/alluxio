package alluxio.client.file.cache.RL;
import java.util.*;

public class QLearning extends RLAgent{

	private double mLearningRate;

	private Map<State, Map<Action, Double>> mQTable = new HashMap<>();


	private double getQvalue(State s, Action a) {
		double res = 0;
		if(!mQTable.containsKey(s)) {
			mQTable.put(s, new HashMap<>());
		} else {
			Map<Action, Double> tmp = mQTable.get(s);
			if(tmp.containsKey(a)) {
				res = tmp.get(mCurrentAction);
			}
		}
		return res;
	}

	private void putQvalue(State s, Action a, double newValue) {
		mQTable.get(s).put(a, newValue);
	}

	public State getState(State state) {
		State newState = new State(state.mLowize + mCurrentAction
			.mLowChangeSize, state.mHighSize + mCurrentAction.mHighChangeSize
		,state.mFullSize + mCurrentAction.mFullChangeSize);

	  return newState;
	}

	private void freshState() {
		mLastState = mCurrentState;
	//	mCurrentAction = getAction(mCurrentState);
		mCurrentState = getState(mLastState);
	}

	public void init(double low, double high, double full) {
     mLastState = new State(low, high, full);
    // mCurrentAction = new
	}

	private Action getMaxQValue(State s) {
		Map<Action, Double> m = mQTable.get(s);
    return null;
	}

	@Override
	public void execute() {
		simpleCurrentReward();
		double mOldQValue, mMaxValue, mNewQvalue;
		mOldQValue = getQvalue(mLastState, mCurrentAction);
		mMaxValue = getQvalue(mCurrentState, mCurrentAction);
		mNewQvalue = (1-mLearningRate) * mOldQValue + mLearningRate * (mCurrentReward + mDelay * mMaxValue);
		putQvalue(mLastState, mCurrentAction, mNewQvalue);
		freshState();
	}
}
