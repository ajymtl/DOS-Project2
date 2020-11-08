# DOS-Project2
Project as a part of curriculum for Distributed Operating Systems Principles -COP5615 (Fall'20)

## How to Run:
dotnet fsi --langversion:preview .\project2.fsx numNodes topology algorithm

## What is working: 
Both algorithms for all 4 topologies. Did not implement the bonus part.

## Largest Network Handled:
|		Algorithm/Topology | Full | Line | 2D | Imp2D |
|---|---|---|---|---|
| gossip	| 100k | 20k  | 50k| 50k   |
| push-sum 	| 20k  | 1k   | 1k | 10k   |
