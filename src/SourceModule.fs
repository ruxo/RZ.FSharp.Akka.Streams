module RZ.FSharp.Akka.Streams.Source

open System
open System.Collections.Generic
open System.Threading.Tasks
open Akka.Streams
open Akka.Streams.Dsl
open Akka.Event
open SourceExtensions

let inline aggregate(zero: 'Result, mapper: 'Result -> 'Out -> 'Result) (this: Source<'Out,'Mat>) =
    this.Aggregate(zero, fun last item -> mapper last item)

let inline aggregateAsync(zero: 'Result, mapper: 'Result -> 'Out -> Task<'Result>) (this: Source<'Out,'Mat>) =
    this.AggregateAsync(zero, fun last item -> mapper last item)

let inline asSourceWithContext(fn: 'Out -> 'Context) (this: Source<'Out,'Mat>) =
    this.AsSourceWithContext(fun item -> fn item)

let inline batch(max, seed: 'Out -> 'Out2, aggregate: 'Out2 -> 'Out -> 'Out2) (this: Source<'Out,'Mat>) =
    this.Batch(max, (fun item -> seed item), fun last item -> aggregate last item)

let inline batchWeighted(max,costFunction: 'Out -> int64, seed: 'Out -> 'Out2, aggregate: 'Out2 -> 'Out -> 'Out2) (this: Source<'Out,'Mat>) =
    this.BatchWeighted(max, (fun item -> costFunction item), (fun item -> seed item), fun last item -> aggregate last item)

let inline collect(isDefined: 'Out -> bool, collection: 'Out -> 'Out2) (this: Source<'Out,'Mat>) =
    this.Collect((fun item -> isDefined item), fun item -> collection item)

let inline combine(first, second, strategy: int -> IGraph<UniformFanInShape<'Out,'c>,Akka.NotUsed>, [<ParamArray>] rest) (this: Source<'Out,'Mat>) =
    this.Combine(first, second, (fun i -> strategy i), rest)

let inline combineMaterialized(other, strategy: int -> IGraph<UniformFanInShape<'Out,'Out2>,Akka.NotUsed>, combineMaterializer: 'Mat -> 'Mat2 -> 'Mat3) (this: Source<'Out,'Mat>) =
    this.CombineMaterialized(other, (fun item -> strategy item), (fun mat1 mat2 -> combineMaterializer mat1 mat2))

let inline concatMany(flatten: 'Out -> IGraph<SourceShape<'T>,'Mat>) (this: Source<'Out,'Mat>) =
    this.ConcatMany(fun item -> flatten item)

let inline concatMaterialized(that, materializedFunction: 'Mat -> 'Mat2 -> 'Mat3) (this: Source<'Out,'Mat>) =
    this.ConcatMaterialized(that, fun m1 m2 -> materializedFunction m1 m2)

let inline conflate(aggregate: 'Out -> 'Out -> 'Out) (this: Source<'Out,'Mat>) =
    this.Conflate(fun last item -> aggregate last item)

let inline conflateWithSeed(seed: 'Out -> 'Seed, aggregate: 'Seed -> 'Out -> 'Seed) (this: Source<'Out,'Mat>) =
    this.ConflateWithSeed((fun item -> seed item), fun last item -> aggregate last item)

let inline expand(extrapolate: 'Out -> IEnumerator<'T>) (this: Source<'Out,'Mat>) =
    this.Expand(fun item -> extrapolate item)
        
let inline groupBy(maxSubstreams, groupingFunction: 'Out -> 'Key) (this: Source<'Out,'Mat>) =
    this.GroupBy(maxSubstreams, fun i -> groupingFunction i)

let inline interleaveMaterialized(graph, segmentSize, combine: 'Mat -> 'Mat2 -> 'Mat3) (this: Source<'Out,'Mat>) =
    this.InterleaveMaterialized(graph, segmentSize, fun m m2 -> combine m m2)
    
let inline keepAlive(timeout, injectElement: unit -> 'Out) (this: Source<'Out,'Mat>) =
    this.KeepAlive(timeout, fun () -> injectElement())

let inline limitWeighted(max, costFunc: 'Out -> int64) (this: Source<'Out,'Mat>) =
    this.LimitWeighted(max, fun item -> costFunc item)

let inline log(name, extract: ('Out -> obj) option, log: ILoggingAdapter option) (this: Source<'Out,'Mat>) =
    this.Log( name,
                (extract |> Option.map (fun f -> Func<'Out,obj>(f)) |> Option.defaultValue null),
                log |> Option.defaultValue null)

let inline mapMaterializedValue(mapFunc: 'Mat -> 'Mat2) (this: Source<'Out,'Mat>) =
    this.MapMaterializedValue(fun m -> mapFunc m)

let inline mergeMany(breadth, flatten: 'Out -> IGraph<SourceShape<'Out2>, 'Mat>) (this: Source<'Out,'Mat>) =
    this.MergeMany(breadth, fun i -> flatten i)

let inline mergeMaterialized(that, combine: 'Mat -> 'Mat2 -> 'Mat3) (this: Source<'Out,'Mat>) =
    this.MergeMaterialized(that, fun m m2 -> combine m m2)

let inline mergeSorted(other, orderFunc: 'Out -> 'Out -> int) (this: Source<'Out,'Mat>) =
    this.MergeSorted(other, fun u v -> orderFunc u v)

let inline monitor(combine: 'Mat -> IFlowMonitor -> 'Mat2) (this: Source<'Out,'Mat>) =
    this.Monitor(fun m monitor -> combine m monitor)

let inline orElseMaterialized(secondary, materializedFunction: 'Mat -> 'Mat2 -> 'Mat3) (this: Source<'Out,'Mat>) =
    this.OrElseMaterialized(secondary, fun m m2 -> materializedFunction m m2)

let inline recover(partialFunc: exn -> 'Out option) (this: Source<'Out,'Mat>) =
    this.Recover(fun ex -> match partialFunc ex with
                            | Some v -> Akka.Util.Option(v)
                            | None -> Akka.Util.Option.None)

let inline recoverWithRetries(partialFuncs: exn -> IGraph<SourceShape<'Out>,'Mat>, attempt) (this: Source<'Out,'Mat>) =
    this.RecoverWithRetries((fun ex -> partialFuncs ex), attempt)

let inline runAggregate(zero, mapper, materializer) (this: Source<'Out,'Mat>) = this.runAggregate(zero, mapper, materializer)
let inline runAggregateAsync(zero, mapper, materializer) (this: Source<'Out,'Mat>) = this.runAggregateAsync(zero, mapper, materializer)
let inline runForeach(action, materializer) (this: Source<'Out,'Mat>) = this.runForeach(action, materializer)
let inline runSum(reduce, materializer) (this: Source<'Out,'Mat>) = this.runSum(reduce, materializer)

let inline select(mapper: 'Out -> 'c) (this: Source<'Out,'Mat>) =
    this.Select(fun x -> mapper(x))

let inline selectAsync(numParallel, mapper: 'Out -> Async<'c>) (this: Source<'Out,'Mat>) =
    this.SelectAsync(numParallel,fun x -> mapper(x) |> Async.StartAsTask)

let inline scan(zero: 'Result, mapper: 'Result -> 'Out -> 'Result) (this: Source<'Out,'Mat>) =
    this.Scan(zero, fun last item -> mapper last item)

let inline scanAsync(zero: 'Result, mapper: 'Result -> 'Out -> Task<'Result>) (this: Source<'Out,'Mat>) =
    this.ScanAsync(zero, fun last item -> mapper last item)

let inline selectAsyncUnordered(parallelism, asyncMapper: 'Out -> Task<'NewOut>) (this: Source<'Out,'Mat>) =
    this.SelectAsyncUnordered(parallelism, fun i -> asyncMapper i)

let inline selectError(selector: exn -> exn) (this: Source<'Out,'Mat>) =
    this.SelectError(fun ex -> selector ex)

let inline selectMany(mapConcater: 'Out -> 'NewOut seq) (this: Source<'Out,'Mat>) =
    this.SelectMany(fun i -> mapConcater i)

let inline skipWhile(predicate: 'Out -> bool) (this: Source<'Out,'Mat>) =
    this.SkipWhile(fun i -> predicate i)

let inline splitAfter(predicate: 'Out -> bool) (this: Source<'Out,'Mat>) =
    this.SplitAfter(fun i -> predicate i)

let inline splitAfter2(substreamCancelStrategy, predicate: 'Out -> bool) (this: Source<'Out,'Mat>) =
    this.SplitAfter(substreamCancelStrategy, fun i -> predicate i)

let inline splitWhen(predicate: 'Out -> bool) (this: Source<'Out,'Mat>) =
    this.SplitWhen(fun i -> predicate i)

let inline splitWhen2(substreamCancelStrategy, predicate: 'Out -> bool) (this: Source<'Out,'Mat>) =
    this.SplitWhen(substreamCancelStrategy, fun i -> predicate i)

let inline statefulSelectMany(mapConcaterFactory: unit -> 'Out -> 'NewOut seq) (this: Source<'Out,'Mat>) =
    this.StatefulSelectMany(fun _ -> Func<'Out,'NewOut seq>(mapConcaterFactory()))

let inline sum(reduce: 'Out -> 'Out -> 'Out) (this: Source<'Out,'Mat>) =
    this.Sum(fun last i -> reduce last i)

let inline takeWhile(predicate: 'Out -> bool, inclusive) (this: Source<'Out,'Mat>) =
    this.TakeWhile((fun i -> predicate i), inclusive)

let inline throttle(cost, per, minimumBurst, calculateCost: 'Out -> int, mode) (this: Source<'Out,'Mat>) =
    this.Throttle(cost, per, minimumBurst, (fun i -> calculateCost i), mode)

let inline watchTermination(materializerFunction: 'Mat -> Task -> 'Mat2) (this: Source<'Out,'Mat>) =
    this.WatchTermination(fun m t -> materializerFunction m t)

let inline where(predicate: 'Out -> bool) (this: Source<'Out,'Mat>) =
    this.Where(fun i -> predicate i)

let inline whereNot(predicate: 'Out -> bool) (this: Source<'Out,'Mat>) =
    this.WhereNot(fun i -> predicate i)

let inline zipWith(other, combine: 'Out -> 'OtherOut -> 'NewOut) (this: Source<'Out,'Mat>) =
    this.ZipWith(other, fun i oi -> combine i oi)
