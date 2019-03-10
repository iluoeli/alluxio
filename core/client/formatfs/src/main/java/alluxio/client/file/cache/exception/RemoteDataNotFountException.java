package alluxio.client.file.cache.exception;

public class RemoteDataNotFountException extends Exception {

  public RemoteDataNotFountException(Throwable throwable) {
    super(throwable);
  }

  public RemoteDataNotFountException(String errorMessage) {
    super(errorMessage);
  }
}
