package com.utils

import com.google.common.primitives.UnsignedInteger

import java.math.BigInteger

/**
 * This implementation allows each node in the chord ring network to have an unsigned integer value
 * between 0 and 2^m^-1 inclusive.
 * This source implementation was borrowed from the following github repository : https://gist.github.com/ktoso/4363757
 */
class UnsignedInt(val intValue: Int) extends AnyVal {

  import UnsignedInteger._

  def <(o: UnsignedInt): Boolean = compareTo(o) == -1
  def <=(o: UnsignedInt): Boolean = o.intValue == intValue || compareTo(o) == -1
  def >(o: UnsignedInt): Boolean = compareTo(o) == 1
  def >=(o: UnsignedInt): Boolean = o.intValue == intValue || compareTo(o) == 1

  def -(o: Int): UnsignedInt = UnsignedInt((fromIntBits(intValue) minus fromIntBits(o)).intValue)
  def +(o: Int): UnsignedInt = UnsignedInt((fromIntBits(intValue) plus fromIntBits(o)).intValue)
  def *(o: Int): UnsignedInt = UnsignedInt((fromIntBits(intValue) times fromIntBits(o)).intValue)
  def /(o: Int): UnsignedInt = UnsignedInt((fromIntBits(intValue) dividedBy fromIntBits(o)).intValue)

  def bigIntegerValue: BigInteger = fromIntBits(intValue).bigIntegerValue

  def compareTo(o: UnsignedInt): Int =
    fromIntBits(intValue).compareTo(fromIntBits(o.intValue))

  override def toString = s"UnsignedInt[real: ${UnsignedInteger.fromIntBits(intValue).toString}, internal: $intValue]"
}

object UnsignedInt {
  import UnsignedInteger._

  val Zero: UnsignedInt = UnsignedInt(ZERO.intValue)
  val One: UnsignedInt = UnsignedInt(ONE.intValue)
  val Max: UnsignedInt = UnsignedInt(MAX_VALUE.intValue)

  def apply(in: Int)               : UnsignedInt = new UnsignedInt(in)
  def apply(in: String)            : UnsignedInt = new UnsignedInt(valueOf(in).intValue)
  def apply(in: BigInt)            : UnsignedInt = new UnsignedInt(valueOf(in.underlying).intValue)
  def apply(in: BigInteger)        : UnsignedInt = new UnsignedInt(valueOf(in).intValue)
  def apply(in: String, radix: Int): UnsignedInt = new UnsignedInt(valueOf(in, radix).intValue)
}