/***********************************************************************
* Copyright (c) 2015 by Regents of the University of Minnesota.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0 which 
* accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*
*************************************************************************/
package edu.umn.cs.spatialHadoop.util;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;

import org.apache.hadoop.io.Writable;

/**
 * Stores an expandable array of integers
 * @author Ahmed Eldawy
 *
 */
public class IntArray implements Writable, Iterable<Integer> {
  /**Stores all elements*/
  protected int[] array;
  /**Number of entries occupied in array*/
  protected int size;
  
  public IntArray() {
    this.array = new int[16];
  }
  
  public void add(int x) {
    append(x);
  }

  /**
   * Inserts a value at a given position in the array.
   * @param position the index in which the new value will be inserted
   * @param value the value to insert into the array
   */
  public void insert(int position, int value) {
    expand(1);
    System.arraycopy(array, position, array, position+1, size() - position);
    array[position] = value;
    size++;
  }

  public void append(int x) {
    expand(1);
    array[size++] = x;
  }
  
  public void append(int[] xs, int offset, int count) {
    expand(count);
    System.arraycopy(xs, offset, array, size, count);
    this.size += count;
  }
  
  public void append(int[] xs, int offset, int count, int delta) {
    expand(count);
    System.arraycopy(xs, offset, array, size, count);
    if (delta != 0) {
      for (int i = 0; i < count; i++)
        this.array[i + size] += delta;
    }
    this.size += count;
  }
  
  public void append(IntArray another) {
    append(another.array, 0, another.size);
  }

  public void append(IntArray another, int offset, int count) {
    append(another.array, offset, count);
  }

  public void append(IntArray another, int delta) {
    append(another.array, 0, another.size, delta);
  }
  
  public boolean contains(int value) {
    for (int i = 0; i < size; i++) {
      if (array[i] == value) {
        return true;
      }
    }
    return false;
  
  }
  
  /**
   * Ensures that the array can accept the additional entries
   * @param additionalSize
   */
  protected void expand(int additionalSize) {
    if (size + additionalSize > array.length) {
      int newCapacity = Math.max(size + additionalSize, array.length * 2);
      int[] newArray = new int[newCapacity];
      System.arraycopy(array, 0, newArray, 0, size);
      this.array = newArray;
    }
  }
  
  public static void writeIntArray(int[] array, DataOutput out) throws IOException {
    out.writeInt(array.length);
    ByteBuffer bb = ByteBuffer.allocate(1024*1024);
    for (int i = 0; i < array.length; i++) {
      bb.putInt(array[i]);
      if (bb.position() == bb.capacity()) {
        // Full. Write to output
        out.write(bb.array(), 0, bb.position());
        bb.clear();
      }
    }
    // Write whatever remaining in the buffer
    out.write(bb.array(), 0, bb.position());
    bb.clear();
  }
  
  @Override
  public void write(DataOutput out) throws IOException {
    out.writeInt(size);
    ByteBuffer bb = ByteBuffer.allocate(1024*1024);
    for (int i = 0; i < size; i++) {
      bb.putInt(array[i]);
      if (bb.position() == bb.capacity()) {
        // Full. Write to output
        out.write(bb.array(), 0, bb.position());
        bb.clear();
      }
    }
    // Write whatever remaining in the buffer
    out.write(bb.array(), 0, bb.position());
    bb.clear();
  }
  
  public static int[] readIntArray(int[] array, DataInput in) throws IOException {
    int newSize = in.readInt();
    // Check if we need to allocate a new array
    if (array == null || newSize != array.length)
      array = new int[newSize];
    byte[] buffer = new byte[1024*1024];
    int size = 0;
    while (size < newSize) {
      in.readFully(buffer, 0, Math.min(buffer.length, (newSize - size) * 4));
      ByteBuffer bb = ByteBuffer.wrap(buffer);
      while (size < newSize && bb.position() < bb.capacity())
        array[size++] = bb.getInt();
    }
    return array;
  }
  
  @Override
  public void readFields(DataInput in) throws IOException {
    int newSize = in.readInt();
    expand(newSize);
    byte[] buffer = new byte[1024*1024];
    size = 0;
    while (size < newSize) {
      in.readFully(buffer, 0, Math.min(buffer.length, (newSize - size) * 4));
      ByteBuffer bb = ByteBuffer.wrap(buffer);
      while (size < newSize && bb.position() < bb.capacity())
        array[size++] = bb.getInt();
    }
  }
  
  public int size() {
    return size;
  }
  
  public boolean isEmpty() {
    return size == 0;
  }

  /**
   * Returns the underlying array. The returned array might have a length that
   * is larger than {@link #size()}. The values of those additional slots are
   * undefined and should not be used.
   * @return
   */
  public int[] underlyingArray() {
    return array;
  }
  
  /**
   * Converts this IntArray into a native Java array that with a length equal
   * to {@link #size()}.
   * @return
   */
  public int[] toArray() {
    int[] compactArray = new int[size];
    System.arraycopy(array, 0, compactArray, 0, size);
    return compactArray;
  }
  
  public void sort() {
    Arrays.sort(array, 0, size);
  }

  public void insertionSort(Comparator<Integer> c) {
    for (int sortSize = 2; sortSize <= size; sortSize++) {
      int pivot = array[sortSize-1];
      int j = sortSize - 2;
      while (j >= 0 && c.compare(array[j], pivot) > 0) {
        array[j + 1] = array[j];
        j--;
      }
      array[j+1] = pivot;
    }
  }

  public int get(int index) {
    return array[index];
  }

  /**
   * Removes and returns the last element in the array
   * @return
   */
  public int pop() {
    return array[--size];
  }

  /**
   * Returns the last element in the array without removing it.
   * @return
   */
  public int peek() {
    return array[size-1];
  }

  public boolean remove(int value) {
    for (int i = 0; i < size; i++) {
      if (array[i] == value) {
        System.arraycopy(array, i + 1, array, i, size - (i + 1));
        size--;
        return true;
      }
    }
    return false;
  }

  public IntArray clone() {
    IntArray newIntArray = new IntArray();
    newIntArray.size = this.size;
    newIntArray.array = this.array.clone();
    return newIntArray;
  }

  /**
   * Remove all elements in the array.
   */
  public void clear() {
    size = 0;
  }

  @Override
  public Iterator<Integer> iterator() {
    return new IntIterator();
  }

  /**Shrinks the array to contain the given number of elements only*/
  public void resize(int newSize) {
    if (newSize > this.size)
      throw new IllegalArgumentException("The new size cannot be greater than the current size");
    this.size = newSize;
  }

  public void set(int index, int value) {
    if (index >= size)
      throw new ArrayIndexOutOfBoundsException(index);
    array[index] = value;
  }

  class IntIterator implements Iterator<Integer> {
    int i = -1;

    @Override
    public boolean hasNext() {
      return i < size() - 1;
    }

    @Override
    public Integer next() {
      return array[++i];
    }

    @Override
    public void remove() {
      throw new RuntimeException("Not yet supported");
    }
    
  }

  public void swap(int i, int j) {
    int t = array[i];
    array[i] = array[j];
    array[j] = t;
  }
}
