/*
* Project and contact information: http://www.cascading.org/
*
* This file is part of the Cascading project.
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
* http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/

package cascading.tap.hcatalog;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobConfigurable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cascading.hadoop.mapred.InputFormatValueCopier;

public class HCatInputFormatKeyCopier implements InputFormatValueCopier<WritableComparable>, JobConfigurable
  {
  private static final Logger LOG = LoggerFactory.getLogger(HCatInputFormatKeyCopier.class);

  private static final String DEFAULT_COPY_METHOD = "copy";
  private static final String DEFAULT_GETTER_METHOD = "get";
  private static final String DEFAULT_SETTER_METHOD = "set";

  public static final String COPY_METHOD = "cascading.hadoop.mapreduce.HCatInputFormatKeyCopier.copyMethod";
  public static final String GETTER_METHOD = "cascading.hadoop.mapreduce.HCatInputFormatKeyCopier.getterMethod";
  public static final String SETTER_METHOD = "cascading.hadoop.mapreduce.HCatInputFormatKeyCopier.setterMethod";

  private Configuration configuration;

  @Override
  public void configure(JobConf jobConf)
    {
    configuration = jobConf;
    }

  @Override
  public void copyValue( WritableComparable oldValue, WritableComparable newValue )
    {
    // NullWritable is a special case as it's singleton class with not setter
    if( newValue == null || newValue instanceof NullWritable )
      {
      return;
      }
    tryToCopy( oldValue, newValue );
    }

  private void tryToCopy( Object to, Object from )
    {
    String copyMethod = configuration.get( COPY_METHOD, DEFAULT_COPY_METHOD );
    String getMethod = configuration.get( GETTER_METHOD, DEFAULT_GETTER_METHOD );
    String setMethod = configuration.get( SETTER_METHOD, DEFAULT_SETTER_METHOD );
    try
      {
      copy( to, from, copyMethod );
      return;
      }
    catch ( InvocationTargetException e )
      {
      throw new RuntimeException( e.getCause() );
      }
    catch( Exception e )
      {
      LOG.debug( "Unable to copy using {} method. Trying {}/{}...", copyMethod, getMethod, setMethod, e );
      try
        {
        getAndSet( to, from, getMethod, setMethod );
        return;
        }
      catch ( InvocationTargetException ie )
        {
          throw new RuntimeException( ie.getCause() );
        }
      catch( Exception ie )
        {
        LOG.debug( "Unable to copy using {}/{} method.", getMethod, setMethod, e );
        }
      }
    throw new RuntimeException( "Unable to find a suitable " + copyMethod + " method or " + getMethod + "/" + setMethod + " pair to copy objects for " + to.getClass() );
    }

  private static Method findMethod( Class<?> clazz, String name, Class<?> ... paramTypes ) throws NoSuchMethodException
    {
    try
      {
      return clazz.getMethod( name, paramTypes );
      }
    catch ( NoSuchMethodException e )
      {
      throw e;
      }
    catch( Exception e )
      {
      throw new RuntimeException( "Unable to find method " + name + " of " + clazz, e );
      }
    }

  private void copy( Object to, Object from, String copyMethod ) throws NoSuchMethodException, InvocationTargetException
    {
    Method copy = findMethod( to.getClass(), copyMethod, from.getClass() );
    try
      {
      copy.invoke( to, from );
      }
    catch ( InvocationTargetException e )
      {
      throw e;
      }
    catch( Exception e )
      {
      throw new RuntimeException( "Unable to invoke " + copyMethod + " on " + to, e );
      }
    }

  private void getAndSet( Object to, Object from, String getMethod, String setMethod ) throws NoSuchMethodException, InvocationTargetException
    {
    Method get = findMethod( from.getClass(), getMethod );
    Method set = findMethod( to.getClass(), setMethod, get.getReturnType() );
    Object value = null;
    try
      {
      value = get.invoke( from );
      }
    catch ( InvocationTargetException e )
      {
      throw e;
      }
    catch( Exception e )
      {
      throw new RuntimeException( "Unable to invoke " + getMethod + " on " + from, e );
      }

    try
      {
      set.invoke( to, value );
      }
    catch ( InvocationTargetException e )
      {
      throw e;
      }
    catch( Exception e )
      {
      throw new RuntimeException( "Unable to invoke " + setMethod + " on " + to, e );
      }
    }

  }