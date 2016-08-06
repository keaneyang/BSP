package com.bloom.persistence;

import java.util.Iterator;
import javax.persistence.CacheStoreMode;
import javax.persistence.EntityManager;
import javax.persistence.Query;
import javax.persistence.TypedQuery;
import org.eclipse.persistence.jpa.JpaQuery;
import org.eclipse.persistence.queries.Cursor;

public class JPAIterable<T>
  implements Iterable<T>
{
  EntityManager em;
  Query query;
  Iterable<T> it;
  boolean isNative;
  boolean noEvents;
  
  public JPAIterable(EntityManager em, Query query, boolean isNative, boolean noEvents)
  {
    this.em = em;
    this.query = query;
    this.isNative = isNative;
    this.noEvents = noEvents;
    this.it = getResults(query);
  }
  
  public Iterator<T> iterator()
  {
    return this.it.iterator();
  }
  
  public void close()
  {
    closeResults(this.it);
    this.em.clear();
    this.em.close();
  }
  
  private <T> Iterable<T> getResults(Query query)
  {
    if ((query instanceof JpaQuery))
    {
      JpaQuery<T> jQuery = (JpaQuery)query;
      jQuery.setHint("eclipselink.cursor.scrollable.result-set-type", "ScrollInsensitive").setHint("eclipselink.cursor.scrollable", Boolean.valueOf(true)).setHint("eclipselink.cursor.page-size", Integer.valueOf(1000));
      
      jQuery.setHint("eclipselink.cache-usage", "NoCache").setHint("eclipselink.read-only", "True").setHint("javax.persistence.cache.storeMode", CacheStoreMode.BYPASS);
      if (this.noEvents) {
        jQuery.setHint("eclipselink.fetch-group.name", "noEvents");
      }
      final Cursor cursor = jQuery.getResultCursor();
      new Iterable()
      {
        public Iterator<T> iterator()
        {
          return cursor;
        }
      };
    }
    return query.getResultList();
  }
  
  static void closeResults(Iterable<?> list)
  {
    if ((list.iterator() instanceof Cursor)) {
      ((Cursor)list.iterator()).close();
    }
  }
}
