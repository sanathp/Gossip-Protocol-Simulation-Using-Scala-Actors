import akka.actor.Actor
import akka.actor.Kill
import akka.actor.ActorRef
import akka.actor.Cancellable
import akka.actor.Actor._
import akka.actor.Props
import scala.util.Random
import java.util.concurrent.TimeUnit
import scala.concurrent.duration.Duration
import scala.collection.mutable.ArrayBuffer
import akka.dispatch.ExecutionContexts
import scala.concurrent.ExecutionContext
import scala.compat.Platform
import akka.actor.ActorSystem
import akka.actor.Scheduler
import com.typesafe.config.ConfigFactory
import sun.reflect.MagicAccessorImpl

sealed trait Gossip
case class Rumor(Name: Int, Max: Int,topology:String) extends Gossip
case class PushSum(Name: Int,Max: Int,topology:String,S: Double, W: Double) extends Gossip
case class PushSumCompleted(value :Double) extends Gossip
case object PushSumTrigger extends Gossip
case object Completed extends Gossip
case object Received extends Gossip
object Gossips {
  def main(args: Array[String]) {
    
    if (args.length != 3) {
    	println("The Number of parameters should be equal to 3")
    
    } else {
    	//Create the Master
	      var numofNodes = Integer.parseInt(args(0))
	      val topology = args(1)
	       val config = ConfigFactory.parseString(""" 
	     akka{ 
	      loglevel = DEBUG     
	       log-dead-letters = 0
	      log-dead-letters-during-shutdown = on
           }
       """)
	      val mainContext = ActorSystem("Gossip",config)
	      //For 2D and imperfect2D topology the number of nodes should be a perfect square
	      if ("2D".equalsIgnoreCase(topology) || "imperfect2D".equalsIgnoreCase(topology)) {
	        numofNodes = getPerfectSquare(numofNodes)
	      }
	      
	      val master = mainContext.actorOf(Props(new GossipMaster(numofNodes, topology, args(2))), "Master")
    }
  }
		  //returns the Nearest perfect square for a number given
		   def getPerfectSquare(n: Int): Int = {
		    var result = 1
		    var i = 1;
		    while (i * i <= n) {
		      result = i * i
		      i = i + 1
		    }
		    result
		  }

}

//Master Actor
class GossipMaster(numNodes: Int, topology: String, algo: String) extends Actor {
	
	  val n = numNodes
	  var startTime:Long = 0
	  var endTime:Long = 0
	  var actors: Array[ActorRef] = new Array[ActorRef](numNodes)
	  var receivedCount = 0
	  var completedCount = 0
	  var childDoneState = Array.fill[Boolean](n)(false)
	  var print:Boolean=true
	  println("Number of Nodes : "+numNodes) 
	  //Create the actors for number of nodes given and store them in an array
	  for (i <- 0 to n - 1) {
	
	    actors(i) = context.system.actorOf(Props(new Node(self,i,1)), "Node" + i.toString)
	
	  }
	  	  //select a node to start the gossip or Push Sum
		  val StartNode= context.system.actorSelection("/user/Node" + 1)
		  if(algo.equals("Gossip")){
		    StartNode ! Rumor(1, numNodes,topology)
		  }else if(algo.equals("PushSum")){
		    StartNode ! PushSum(1,numNodes,topology,0,0)
		    self ! PushSumTrigger
		  }
		  startTime = System.currentTimeMillis()
		  
		  def receive = {
			//Checks whether the rumor has been spread successfully or not
		    case Completed => {		
		      completedCount = completedCount + 1
		      if (completedCount == numNodes) {
			        endTime=System.currentTimeMillis()
			        println("Gossip spread successfully for " + topology +" topology Successfully")
			        endTime= endTime-startTime
			        println("Time taken to complete : "+endTime+" ms")
			        context.system.shutdown()
		      }
		    }
		    //Triggers the process of Push Sum
		     case PushSumTrigger=>{
		        for (i <- 0 until n - 1) {
		        actors(i) ! PushSum(i,numNodes,topology,0,1)
		      }		       
		     }
		     case Received=>{
		       completedCount = completedCount + 1
		      if (completedCount == numNodes) {
		    	  self!PushSumTrigger
		    	  completedCount=0
		      }
		     }
		     //Prints message when push Sum s /w is converged
		      case PushSumCompleted(value)=>{
		        if(print)
		        {
		        
		        endTime=System.currentTimeMillis()
			    println("PushSum Completed for " + topology +" topology ")
			    println("Coverged at "+value)
			    endTime= endTime-startTime
			    println("Time taken to complete : "+endTime+" ms")
		        context.system.shutdown() 
		        print=false
		        }
		     }
		  }
}

class Node(master: ActorRef, s: Int, w: Int) extends Actor {
		  import context._
		  var rumorCount: Int = _
		  val name = self.path.name
		  var rumor = ""
		  var send:Boolean=false
		  var sVal: Double = s
		  var wVal: Double = w
		  var currentVal:Double=0
		  var previousVal:Double=0
		  var pushSumCount:Int=0
		  var pushSumConvergeCount:Int=0
		  var k:Int=0
		  var noNodes:Int=0

		  def receive = {
		    case Rumor(l, max,topology) => {
		      
		      if(self.path.name!=sender.path.name)
		      {
			      rumorCount = rumorCount + 1
			      if (rumorCount == 1) {		        
			        master ! Completed
			      }
		      }
		      var number = 0
		      var number2d :Int=l
		      //select the neighbor randomly based on the topology 
		      if ("line".equalsIgnoreCase(topology)) {
			        var maxi=max-1
			       
			        if (l == 0) {
			          number = l + 1
			        } else if (l == maxi) {
			          number = l - 1
			        } else {
			          var next = Random.nextInt(2)
			          if (next == 0) {
			            number = l + 1
			          } else {
			            number = l - 1
			          }
		        }
		      }else if("full".equalsIgnoreCase(topology))
		      {
				       var d = Random.nextInt(max+1)
				       number = d;        
		      }else if ("2D".equalsIgnoreCase(topology) || "imperfect2D".equalsIgnoreCase(topology)) {
			          val r = Math.sqrt(max + 1);
			          var d = r.toInt
			          var row = (number2d + 1) / d;
			          var col = (number2d + 1) % d - 1;
			          var ran = Random.nextInt(4)
			          if("imperfect2D".equalsIgnoreCase(topology))
			          {
			        	  ran = Random.nextInt(5)
			          }
			          if (ran == 0) {
			            row = row - 1
			          } else if (ran == 1) {
			            row = row + 1
			          } else if (ran == 2) {
			            col = col - 1
			          } else if(ran==3) {
			            col = col + 1
			          } else{
			            var temp:Int=row
			            row=col
			            col=temp
			          }
			          if (row >= d) row = row - 2;
			          if (col >= d) col = col - 2;
			          if (row < 0) row = row + 2;
			          if (col < 0) col = col + 2;
			
			          number2d = row * d + (col + 1)
			          number2d = number2d - 1;
			
			          number=number2d
		        }
		      	
		        val targetG = context.actorSelection("/user/Node" + number)
		        val msg = number
		        //send the rumor to neighbor 
		        targetG ! Rumor( number, max,topology)
		        if(rumorCount<11){
		           self ! Rumor( l, max,topology)
		          }
		
		    }
		    
		    case PushSum(l,max,topology,s,w) => {
		    	noNodes=max
		    	k=l
		    	if(s==0&&w==0)
		    	{
			    	  send=true
			    	  sVal = sVal / 2
			          wVal = wVal / 2
			          currentVal =sVal/wVal
			    	  pushSumCount =pushSumCount +1
		    	} 
		    	else if(s==0&&w==1){
		    	  master!Received
			    	  if(pushSumCount!=0)
			    	  {
			    	   send=true
			    	  sVal = sVal / 2
			          wVal = wVal / 2
			          currentVal =sVal/wVal
			    	  pushSumCount =pushSumCount +1 
			    	  }else{
			    	  send=false
			    	  }
		    	}
		    	else{
			    	  send=true
			    	  sVal=sVal+s
			          wVal=wVal+w	
			          sVal = sVal / 2;
			          wVal = wVal / 2;
			          pushSumCount =pushSumCount +1
		    	}
		    	currentVal =sVal/wVal
		    	if(currentVal==previousVal){
				       pushSumConvergeCount =pushSumConvergeCount +1
				    	  
				 }else{
				    pushSumConvergeCount =0
				    previousVal=currentVal 
				 }
		    	
		    	if(pushSumConvergeCount==3)
		    	{
		        master!PushSumCompleted(currentVal)
		    	}
		    	
		    	if(send){
			      var number = 0
			      var number2d :Int=l
			      if ("line".equalsIgnoreCase(topology)) {
			        var maxi=max-1
			       
			        if (l == 0) {
			          number = l + 1
			        } else if (l == maxi) {
			          number = l - 1
			        } else {
			          var next = Random.nextInt(2)
			          //println("Random is " + next)
			          if (next == 0) {
			            number = l + 1
			          } else {
			            number = l - 1
			          }
			        }
			      }else if("full".equalsIgnoreCase(topology))
			      {
			    	  var d = Random.nextInt(max)
			    	  number = d;        
			      }else if ("2D".equalsIgnoreCase(topology) || "imperfect2D".equalsIgnoreCase(topology)) {
			          val r = Math.sqrt(max + 1);
			          var d = r.toInt
			          var row = (number2d + 1) / d;
			          var col = (number2d + 1) % d - 1;
			          var ran = Random.nextInt(4)
			          if("imperfect2D".equalsIgnoreCase(topology))
			          {
			        	  ran = Random.nextInt(5)
			          }
			          if (ran == 0) {
			            row = row - 1
			          } else if (ran == 1) {
			            row = row + 1
			          } else if (ran == 2) {
			            col = col - 1
			          } else if(ran==3) {
			            col = col + 1
			          } else{
			            var temp:Int=row
			            row=col
			            col=temp
			          }
			          if (row >= d) row = row - 2;
			          if (col >= d) col = col - 2;
			          if (row < 0) row = row + 2;
			          if (col < 0) col = col + 2;
			
			          number2d = row * d + (col + 1)
			          number2d = number2d - 1;
			
			          number=number2d
			        }
		        val targetG = context.actorSelection("/user/Node" + number)
		        val msg = number
		                
		         targetG ! PushSum(number,max,topology,sVal, wVal)
		         
		    }
 
		    }
		
		  }
}
