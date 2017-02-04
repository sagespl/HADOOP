package pl.com.sages.hadoop.scalatutorial

class Person {

  // You must initialize the field
  var age = 0

  // Methods are public by default
  def increment() {
    age += 1
  }

  def current() = age

  def currentValue = age

}
