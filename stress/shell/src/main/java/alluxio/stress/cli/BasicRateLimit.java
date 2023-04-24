package alluxio.stress.cli;

public interface BasicRateLimit {
  double acquire();

  boolean tryAcquire();
}
