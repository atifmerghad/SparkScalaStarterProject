val numberOne : Int =1
val truth : Boolean = true
val char : Char = 'a'
val float : Float = 1.2
val pi : Double = 12.432434
val small : Byte = 127

println("Here is a mess: "+truth)
print("Here is a mess: ",truth)
println(f"Pi is about $pi%.3f")
println(f"Zero padding on the left : $numberOne%05d")
println(s"I can use he prefix s  use var like $numberOne $truth $char")

var ultimateAnswer : String = "To life, the universe, and everything is 42"
val pattern = """.*([\d]+).*""".r
val pattern(answerString) = ultimateAnswer
val answer = answerString.toInt
println(answer)

// Boolean
val isGreater = 1>2
val isLesser = 1>2
val impossible = isGreater & isLesser
val anotherWay = isGreater & isLesser
val isBest : Boolean = "Atif" == "Atif"
