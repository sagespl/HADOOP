package pl.com.sages.hadoop.scalatutorial

object Main {

  def main(args: Array[String]): Unit = {
    println("Main method ob object Main")

    val personOld = new Person(5)
    val person = new Person(5, 12)

    person.age = 12
    person.age_=(12)
    println(person.age)

    person.increment()
    println(person.age)
    println(person.current())
    println(person.current)
    println(person.currentValue)

    person.setName("Name")
    println(person.getName)

  }

}
