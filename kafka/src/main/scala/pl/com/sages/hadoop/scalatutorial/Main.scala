package pl.com.sages.hadoop.scalatutorial

object Main {

  def main(args: Array[String]): Unit = {
    println("Main method ob object Main")

    val person = new Person

    person.age = 12
    person.age_=(12)
    println(person.age)

    person.increment()
    println(person.age)
    println(person.current())
    println(person.current)
    println(person.currentValue)

  }

}
