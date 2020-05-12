package org.restfeeds.client;

import java.util.Optional;

public interface LockProvider {

  Optional<Lock> lock(LockingParams lockingParams);

}
