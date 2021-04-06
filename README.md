# RZ.FSharp.Akka.Streams

Akka.net stream extension for F#.  Basically, create inline extension methods for Akka.Stream.Source.
All F# compatible methods starts with lower case. For example:

```
let source = Source.From(seq { 1..100 })

// instead of source.RunForeach(i => Console.WriteLine(i), materializer); in C#
source.runForeach((printfn "%i"), materializer)
```
