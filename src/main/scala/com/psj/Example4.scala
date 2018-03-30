package com.psj

import org.apache.spark.sql.SparkSession


object Example4 {

  def main(args: Array[String]): Unit = {

    def discountedPrice(price: Double, rate: Double): Double = {
      var discount = price * rate
      var returnValue = price - discount
      returnValue
    }

    var orgrRate = 0.2
    var orgPrice = 2000
    var newrPrice =
      discountedPrice(orgPrice, orgrRate)


    def test(a: Double, b: Int): Double = {
      var round1 = math.round(a * math.pow(10, b)) / math.pow(10, b)
      return round1
    }


    def test2(a: Double, a2: Int): Double = {
      var round2 = math.pow(10, a2)
      var round1 = math.round(a * math.pow(10, round2)) / round2
          return round1
        }


    var a = 15.125222
    var b = 15.147218
    var c = 69.72756

    var ra = math.round(a * 100) / 100.0
    var rb = math.round(b * 100) / 100.0
    var rc = math.round(c * 100) / 100.0

    var abcSum = ra + rb + rc

    var abcSum1 = 100 - abcSum

    var NewA = a + abcSum1
    var NewAr = math.round(NewA * 100) / 100.0

    var Newsum = NewAr + rb + rc

    def Around1(a1: Double, b1: Int): Double = {
      var testPow = math.pow(10, b1)
      var testRound = math.round(a1*testPow)/testPow
      return testRound
    }
    var ra1 = Around1(a,2)
    var rb1 = Around1(b,2)
    var rc1 = Around1(c,2)

    var abcsum1 = ra1+rb1+rc1
    var abcsum2 = 100-abcsum1
    var newa = a +(abcsum2)
    var newra = Around1(newa,2)

    var newsum = newra + rb1 + rc1

    var a = 1
  }


}
