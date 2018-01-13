package io.github.reggert.cumulative.core.data

import java.util
import util.AbstractMap.SimpleImmutableEntry

import org.apache.accumulo.core.data.{Key, Value}
import org.apache.accumulo.core.util.format.DefaultFormatter


/**
  * Immutable representation of an entry in an Accumulo table.
  *
  * Note that the `toString` method is implemented using Accumulo's [[DefaultFormatter]], so entries will
  * be displayed as they would in the Accumulo shell.
  *
  * @param key the entry key.
  * @param value the entry value.
  */
final case class Entry(key : EntryKey, value : EntryValue) {
  /**
    * Converts this to a [[util.Map.Entry]] containing an Accumulo [[Key]] and [[Value]].
    */
  def toAccumuloEntry : util.Map.Entry[Key, Value] =
    new SimpleImmutableEntry(key.toAccumuloKey, value.toAccumuloValue)

  override def toString : String = DefaultFormatter.formatEntry(toAccumuloEntry, key.timestamp.isSpecified)
}


object Entry {
  /**
    * Constructs an entry from a [[util.Map.Entry]] containing an Accumulo [[Key]] and [[Value]].
    * @param entry the entry to convert.
    * @return a new Entry.
    */
  def apply(entry : util.Map.Entry[Key, Value]) : Entry =
    Entry(EntryKey(entry.getKey), EntryValue(entry.getValue))
}
