
def squareIt(x: Int): Int = { x*x }

def cubeIt(x: Int): Int = { x*x*x }

println(squareIt(2))
println(cubeIt(2))

def transformInt( x: Int, f: Int => Int ) : Int = { f(x) }
val result = transformInt(2, cubeIt)
transformInt(3, { x => x*x*x})
transformInt(3, { x => x/2})
transformInt(2, { x => {val y=x*2; y*y}})


// Data Structures

// Tuples

// Immutable lists

val captainStuff = ("Picard", "Entreprise-D", "NCC-1701-D")
println(captainStuff)
println(captainStuff._1)
println(captainStuff._2)
println(captainStuff._3)

val aBunchofStaff = ("Kirk", 1994, true)

val shipList= List("Entreprise", "Defiant", "Voyager", "Deep")

println(shipList(1))
println(shipList.head)
println(shipList.tail)

for( ship <- shipList) { println(ship) }
val backwardShips = shipList.map( (ship : String) => {ship.reverse} )
for( ship <- backwardShips) { println(ship) }

// reduce() to combine together all the items in a collection using some function
val numList = List(1,2,3,4,5)
val sum = numList.reduce( (x : Int , y : Int) => x+y )
// filter remove stuff
val iHateFives = numList.filter((x: Int) => x!=5 )
val iHateThrees = numList.filter( _!=3 )

// Concatenate  list
val moreNumbers = List(6,7,8)
val lotOfNumbers = numList ++  moreNumbers

val reversed  = numList.reverse
val sorted  = reversed.sorted

val lotOfDuplicates = numList ++ numList
val distinctValues  = lotOfDuplicates.distinct
val maxValue = numList.max
val total = numList.sum
val hasThree = iHateThrees.contains(3)

// Maps
val shipMap = Map("Kirk" -> "Entreprise", "Picard" -> "Entreprise-D","Sisko" -> "Deep Space nine", "Jan"-> "Voyager")
println(shipMap("Jan"))
println(shipMap.contains("Jan"))
val archerShip = util.Try(shipMap("Archer")) getOrElse "Unknown"
println(archerShip)
