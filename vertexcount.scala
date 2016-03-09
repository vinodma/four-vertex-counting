/* The code below counts the number of different motif patterns based on path sampling 
from this paper 
http://www.www2015.it/documents/proceedings/proceedings/p495.pdf
*/

import org.apache.spark.graphx._
import org.apache.spark.graphx.util._
import scala.util.Random
import scala.collection.mutable.ListBuffer
//number of samples
var numSamples = 200
//sample graph
var gmat = GraphGenerators.rmatGraph(sc,3000,3000)

var g=gmat.subgraph(epred=e=>e.srcId!=e.dstId)

val g1=g.joinVertices(g.degrees)((vid,deg,u)=>u)

val tau_e= g1.mapTriplets(trip=>((trip.srcAttr-1)*(trip.dstAttr-1)).toDouble)
val W = tau_e.edges.map(a=>a.attr).reduce(_+_).toDouble


 
var counts = new ListBuffer[String]()
var Ci = new ListBuffer[Double]()

for(i<-1 to 200){
	counts+=sample(g1,W)
}
//val keys = counts.toList.removeDuplicates
val keys = List("-1","1","2","3","4","5","6")
val hist = Map.empty[String, Int] ++ keys.map{ k => (k, (counts count (_==k)))} 
val A2i = List(0,1,2,4,6,12)

for(i<-2 to 6)
{
	Ci += (hist((i).toString)/numSamples.toDouble)*(W/A2i(i-1))
}

 val deg_coll = gmat.degrees.map(d=> (d._2,1)).reduceByKey(_+_)
 val N1 = deg_coll.map(x=>binomial(x._1,3)*x._2).reduce(_+_)
 val C1 = N1.toDouble - Ci(1) - 2.0*Ci(3) - 4.0*Ci(4)
 
 Ci =  C1+=:Ci

 /*factorial N*/
 def factorial(n: BigInt): BigInt = {  
      if (n <= 1)
         1  
      else    
      n * factorial(n - 1)
   }

   /*Binomial n,m */
   def binomial(n:BigInt,m:BigInt):BigInt = {
   	factorial(n)/(factorial(m)*factorial(n-m))
   }

   /*match the type of motif  */
   def matchmotif(motif:Graph[Int,Int]):String= {
	   try{
   println(motif.edges.count.toString)
   motif.edges.count match {   
   	case 3 => if(motif.degrees.map(a=>a._2).reduce((a,b)=>if(a<b) a else b ) == 1) "2" else "-1"
   	case 4 => if(motif.degrees.map(a=>a._2).reduce((a,b)=>if(a>b) a else b ) == 2) "4" else "3"
   	case 5 => "5"
   	case 6 => "6"
   	case _ => "-1"
   }
} catch 
{
	case e:Exception => "-2"
}

}

/*path sampling from graph */
def sample(g1:Graph[Int,Int],W:Double):String = {
	try{
val tau_e= g1.mapTriplets(trip=>Math.pow(Random.nextDouble(),W/((trip.srcAttr-1)*(trip.dstAttr-1)).toDouble))

val sampedge = tau_e.edges.map(x=>x).takeOrdered(1)(Ordering[Double].reverse.on(x=>x.attr))

val u = sampedge(0).srcId

val v = sampedge(0).dstId

val src_eds=g.edges.filter(e=> (e.srcId==u||e.dstId==u))

val src_verts=src_eds.flatMap(e=>List(e.srcId,e.dstId)).filter(x=>(x!=u&&x!=v))

val dst_eds=g.edges.filter(e=> (e.srcId==v||e.dstId==v))

val dst_verts=dst_eds.flatMap(e=>List(e.srcId,e.dstId)).filter(x=>(x!=u&&x!=v))

val u1 = src_verts.takeSample(false,1)

val v1 = dst_verts.takeSample(false,1)

val motif= g.subgraph(vpred=(vid,_)=>(vid==u)||(vid==v)||(vid==u1(0))||(vid==v1(0)))
val motiftype = matchmotif(motif)
motiftype
}
catch {
	case e:Exception => "-2"
}
}







