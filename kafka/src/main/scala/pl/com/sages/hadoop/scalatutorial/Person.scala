package pl.com.sages.hadoop.scalatutorial

import scala.beans.BeanProperty

class Person(var id: Int) {

  println("test")

  def this(id: Int, age: Int) {
    this(id)
    this.age = age
  }

  // You must initialize the field
  var age = 0

  @BeanProperty var name = ""

  // Methods are public by default
  def increment() {
    age += 1
  }

  def current() = age

  def currentValue = age

}
