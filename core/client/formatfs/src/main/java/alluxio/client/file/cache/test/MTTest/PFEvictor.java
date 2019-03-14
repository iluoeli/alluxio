package alluxio.client.file.cache.test.MTTest;

import org.apache.commons.math.linear.Array2DRowRealMatrix;
import org.apache.commons.math.linear.LUDecomposition;
import org.apache.commons.math.linear.LUDecompositionImpl;
import org.apache.commons.math.linear.RealMatrix;

public class PFEvictor {
  double [][] benefitVector;
  private int userNum;
  private int fileNum;

  public void init(int a, int b) {
    userNum = a;
    fileNum = b;
    benefitVector = new double[a][b];
  }

  public void getMatrix() {
    RealMatrix testmatrix1 = new Array2DRowRealMatrix(benefitVector);
    RealMatrix inversetestMatrix = inverseMatrix(testmatrix1);


  }

  public static RealMatrix inverseMatrix(RealMatrix A) {
    RealMatrix result = new LUDecompositionImpl(A).getSolver().getInverse();
    return result;
  }
}
