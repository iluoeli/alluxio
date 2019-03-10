package alluxio.client.file.cache.RL;

public class ActorCritic extends RLAgent{

	private int mStateLength = 4;
	private int mActionLength = 3;
	private double [] mValueResidual = new double[mStateLength];
	private double [][] mStateResidual = new double[mActionLength][mStateLength];
	private int mTDStep = 1;
	private double[] mCurrentState;
	private double[] mLastState;
	private double[] mAction;
	private double[][] mStateParameter = new double[mActionLength][mStateLength];
	private double[] mValueParameter = new double[mStateLength];
	private double mValueLearningRate;
	private double mStateLearningRate;
	private Gradient mGradient = new GaussGradient();

	/**
	 * Get state value by state
	 */
	private double getStateValue(double[] state, double[] valueParameter) {
    double hitValue = state[4] * valueParameter[4];
    return hitValue / (state[0] * valueParameter[0] +
			state[1] * valueParameter[1] + state[2] * valueParameter[2]);
	}

	private double[] getState(double[] state, double[] action) {
		double [] state1 = new double[state.length];
		state1[0] = state[0] + action[0];
		state1[1] = state[1] + action[1];
		state1[2] = state[2] + action[2];
		return state1;
	}

	public double[] getAction(double[] state, double [][] stateParameter) {
		return mGradient.simpleAction(state, stateParameter);
	}

	private void freshState() {
		mLastState = mCurrentState;
		mAction = getAction(mCurrentState, mStateParameter);
		mCurrentState = getState(mCurrentState, mAction);
	}

	public void execute() {
		simpleCurrentReward();
		mCurrentState[3] = mCurrentHitRatio;
		double newReward = mCurrentReward + mDelay * getStateValue(mCurrentState, mValueParameter) - getStateValue(mLastState, mValueParameter);

		double tmp[] = mGradient.getBaseFunction(mLastState);
		for(int i = 0 ;i < mStateLength ; i ++) {
			mValueResidual[i] = mDelay * mTDStep * mValueResidual[i] + tmp[i];
			mValueParameter[i] = mValueResidual[i] * mValueLearningRate * newReward;
		}
		double[][] gra = mGradient.gradient(mLastState, mAction, mStateParameter);
		for(int i =0 ; i < mActionLength; i ++) {
			for(int j = 0;j < mStateLength; j ++) {
				mStateResidual[i][j] = mTDStep * mStateResidual[i][j] + gra[i][j];
				mStateParameter[i][j] =mStateResidual[i][j] * mStateLearningRate * newReward;
			}
		}
		freshState();
	}
}
