#r "nuget: Akka.FSharp"
#r "nuget: Akka.Remote"

open System
open Akka.Actor
open Akka.Configuration
open Akka.FSharp
open System.Collections.Generic

let mutable count = 0
let system = System.create "System" (Configuration.defaultConfig())
let args = System.Environment.GetCommandLineArgs()
let mutable tasksDone = 0
let mutable stillWorking = true
let mutable shutdownWorkerFlag = false
let random = System.Random()
let mutable actorArray: list<IActorRef> = []
let mutable numActors: int = 0
let mutable timeElapsed: float = 0.0

type SumWorkerState = {
    mutable CurrentSum: float;
    mutable CurrentWeight: float;
    mutable PreviousSum: float;
    mutable PreviousWeight: float;
    mutable Previous2Sum: float;
    mutable Previous2Weight: float;
}

type NeighborInfo = {
    NeighborList: string;
    MyOwnId: int;
}

type WorkerMessage = {
    DoWork: bool;
}

type GossipMessage = {
    Gossip: string;
}

type PushSumMessage = {
    Sum: float;
    Weight: float;
}

type BossMessage = {
    Topology: string;
    Algorithm: string;
    NumNodes: int;
}

type WorkerStatus = {
    WorkerCompleted: bool;
}

type TimeoutBoss = {
    TimeoutBoss: bool;
}

type PartialCompleted = {
    Percentage: int;
}

type CheckCheck = {
    CheckCheck: bool;
}

let nearestSquare (num: int) =
    let exactRoot = sqrt(float num)
    let root = int exactRoot
    root*root

let getNextRandomNumber (size: int) = 
    let random = System.Random()
    random.Next(size)

let neighborsFull (myId: int) =
    let mutable num = getNextRandomNumber(numActors)
    while (num = myId) do
        num <- getNextRandomNumber(numActors)
    num

let neighborsLine (myId: int) = 
    let mutable num = 0
    if myId = 0 then
        num<-1
    elif myId = numActors-1 then
        num<-numActors-2
    else
        let toss = getNextRandomNumber(2)
        if toss = 0 then
            num<-myId-1
        else
            num<-myId+1
    num

let neighbors2D (myId: int, isImperfect: bool) = 
    let mutable neighbors = []
    let root = int(sqrt(float(myId)))
    if (myId-root) > -1 then
        neighbors <- neighbors @ [myId-root]
    if (myId-1) > -1 then
        neighbors <- neighbors @ [myId-1]
    if (myId+1) < numActors then
        neighbors <- neighbors @ [myId+1]
    if (myId+root) < numActors then
        neighbors <- neighbors @ [myId+root]
    if (isImperfect) then
        let mutable num =  getNextRandomNumber(numActors)
        while ((num = myId) || (num = myId-root) || (num = myId-1) || (num = myId+1) || (num = myId+root)) do
            num <- getNextRandomNumber(numActors)
        neighbors <- neighbors @ [num]
    let toss = getNextRandomNumber(neighbors.Length)
    neighbors.[toss]
 
let getNeighbor(id: int, topology: string) = 
    if topology = "full" then
        neighborsFull(id)
    elif topology = "line" then
        neighborsLine(id)
    elif topology = "2D" then
        neighbors2D(id, false)
    else
        neighbors2D(id, true)


type SumWorker() = 
    inherit Actor()
    let desiredRatio = 0.0000000001
    let mutable myNeighbors: string = null
    let mutable iAmDone = false
    let mutable myId: int = 0
    let mutable state: SumWorkerState = {CurrentSum = 0.0; CurrentWeight = 1.0; PreviousSum = 100.0; PreviousWeight = 1.0; Previous2Sum = 100.0; Previous2Weight = 1.0} 
    override x.OnReceive (message:obj) = 
        match message with
        | :? WorkerMessage as m ->
            let worker = actorArray.[getNeighbor(myId, myNeighbors)]
            worker.Tell { Sum = state.CurrentSum/2.0; Weight = state.CurrentWeight/2.0;}
            state.CurrentSum <- state.CurrentSum/2.0
            state.CurrentWeight <- state.CurrentWeight/2.0
        | :? NeighborInfo as m ->
            myNeighbors <- m.NeighborList
            myId <- m.MyOwnId
            state.CurrentSum <- float(myId) + 1.0
        | :? PushSumMessage as m ->
            if not iAmDone then 
                state.Previous2Sum <- state.PreviousSum
                state.Previous2Weight <- state.PreviousWeight
                state.PreviousSum <- state.CurrentSum
                state.PreviousWeight <- state.CurrentWeight
                state.CurrentSum <- state.CurrentSum + m.Sum
                state.CurrentWeight <- state.CurrentWeight + m.Weight
                let Prev2Ratio = state.Previous2Sum / state.Previous2Weight
                let PrevRatio = state.PreviousSum / state.PreviousWeight
                let CurrentRatio = state.CurrentSum / state.CurrentWeight
                if (abs(Prev2Ratio - PrevRatio) < desiredRatio) && (abs(PrevRatio - CurrentRatio) < desiredRatio ) then
                    let boss = system.ActorSelection("akka://System/user/Boss")
                    boss.Tell { WorkerCompleted = true }
                    iAmDone <- true
                let worker = actorArray.[getNeighbor(myId, myNeighbors)]
                worker.Tell { Sum = state.CurrentSum/2.0; Weight = state.CurrentWeight/2.0;}
                state.CurrentSum <- state.CurrentSum/2.0
                state.CurrentWeight <- state.CurrentWeight/2.0
                x.Self.Tell { DoWork = true}
        | _ -> printfn "ERROR IN WORKER"

type Worker() = 
    inherit Actor()
    let mutable gossipCount = 0
    let gossipCountLimit = 10
    let mutable myNeighbors: string = null
    let mutable myId: int = 0
    let bufferRatio = 2;
    override x.OnReceive (message:obj) = 
        match message with
        | :? WorkerMessage as m ->
            if gossipCount <= gossipCountLimit*2 then 
                let worker = actorArray.[getNeighbor(myId, myNeighbors)]
                worker.Tell { Gossip = "Hello" }
                x.Self.Tell { DoWork = true }
        | :? GossipMessage as m -> 
            gossipCount <- gossipCount + 1
            if gossipCount = gossipCountLimit then
                let boss = system.ActorSelection("akka://System/user/Boss")
                boss.Tell { WorkerCompleted = true }
                x.Self.Tell { DoWork = true}
            else
                x.Self.Tell { DoWork = true }
        | :? NeighborInfo as m ->
            myNeighbors <- m.NeighborList
            myId <- m.MyOwnId
        | :? CheckCheck as m ->
            printfn "%s %d" (string x.Self.Path) gossipCount
        | _ -> printfn "ERROR IN WORKER"
     
let spawnWorkers (numNodes: int, algorithm: string) = 
    if algorithm = "gossip" then
        [for i in 0 .. (numNodes-1) do yield system.ActorOf(Props(typedefof<Worker>), "Worker" + (string) i)]
    else
        [for i in 0 .. (numNodes-1) do yield system.ActorOf(Props(typedefof<SumWorker>), "SumWorker" + (string) i)]

type Boss() =
    inherit Actor()
    let mutable numNodes = 0
    let mutable startTime = 0.0
    let mutable endTime = 0.0
    override x.OnReceive (message:obj) =   
        match message with
        | :? BossMessage as msg ->
            numNodes <- msg.NumNodes
            if msg.Topology = "2D" || msg.Topology = "imp2D" then
                numNodes <- nearestSquare(numNodes)
            actorArray <-  spawnWorkers(numNodes, msg.Algorithm)
            numActors <- actorArray.Length
            let topology = msg.Topology
            for i in 0..numActors-1 do
                actorArray.[i].Tell { NeighborList = topology; MyOwnId = i }
            actorArray.[getNextRandomNumber numActors].Tell {DoWork = true}
            startTime <- System.DateTime.Now.TimeOfDay.TotalMilliseconds
        | :? WorkerStatus as msg ->
            if msg.WorkerCompleted then
                tasksDone <- tasksDone + 1 
                if tasksDone = numNodes || (float(tasksDone)/float(numNodes)) > 0.99 then
                    endTime <- System.DateTime.Now.TimeOfDay.TotalMilliseconds
                    timeElapsed <- endTime - startTime
                    stillWorking <- false
        | :? TimeoutBoss as msg ->
            let percentage = tasksDone
            x.Sender.Tell {Percentage = percentage}
        | _ -> printfn "ERROR IN BOSS"

let boss= system.ActorOf(Props(typedefof<Boss>), "Boss")
boss.Tell { NumNodes = args.[3]|>int; Topology = args.[4]; Algorithm = args.[5]; }
let timeoutStart =  System.DateTime.Now.TimeOfDay.TotalMilliseconds
while (stillWorking && (System.DateTime.Now.TimeOfDay.TotalMilliseconds-timeoutStart < 100000.0)) do
    System.Threading.Thread.Sleep 100
if stillWorking then
    printfn "%d" tasksDone
else
    printfn "%A" timeElapsed
boss.Tell(PoisonPill.Instance)