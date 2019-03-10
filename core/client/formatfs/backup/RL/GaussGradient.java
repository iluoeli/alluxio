package alluxio.client.file.cache.RL;

import java.util.Random;

public class GaussGradient implements Gradient {

	//public final double GAUSS_MAX_LENGTH;

	public double[][] gradient(double[] state, double[] action, double [][] mStateParameter) {
		double [][] tmp = new double[action.length][state.length];
		double[] baseState = getBaseFunction(state);
    for(int i = 0 ; i< action.length ; i ++) {
			double averageValue =action[i] - getAverageFunction(mStateParameter[i], baseState);
      for(int j = 0 ;j  < state.length; j ++) {
				tmp[i][j] = averageValue * baseState[j];
			}
		}
		return tmp;
	}

	public double[] simpleAction(double[] state, double [][] stateParameter) {
		double[] res = new double[stateParameter.length];
		double[] baseState = getBaseFunction(state);
		for(int i = 0; i < res.length ; i ++) {
      double variance = 0;
      for(int j = 0; j< stateParameter[i].length; j ++) {
      	variance += stateParameter[i][j] * baseState[j];
			}
			res[i] = getDistribute() + variance;
		}
    return res;
	}

	public double[] getBaseFunction(double[] state) {
    return state;
	}

	private double getAverageFunction(double[] stateParameter, double[] baseState) {
    double res = 0;
		for(int i = 0; i < baseState.length ; i ++) {
    	res += stateParameter[i] * baseState[i];
		}
		return res;
	}

	private double getDistribute() {
		double i = ((double) (new Random().nextInt(10) + 1)) / 10;
		double j = ((double) (new Random().nextInt(10) + 1)) / 10;
		return Math.sqrt(-2 * Math.log(i)) * Math.cos(2 * Math.PI * j);
		//double jj = Math.sqrt(-2 * Math.log(i)) * Math.sin(2 * Math.PI * j);
	}
}
