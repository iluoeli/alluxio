package alluxio.client.file.cache.RL;

public interface Gradient {

	public double[][] gradient(double[] state, double[] action, double[][] parameter) ;

	public double[] simpleAction(double[] state, double[][] stateParameter);

	public double[] getBaseFunction(double[] state);

}
