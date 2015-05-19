package cascading.tap.hive;

/** Provides a means to handle the situation when a held lock fails. */
public interface LockFailureListener
  {

  /** Called when the specified lock has failed. You should probably abort your job in this case. */
  void lockFailed( long lockId, Iterable<HiveTableDescriptor> tableDescriptors, Throwable t );

  }
